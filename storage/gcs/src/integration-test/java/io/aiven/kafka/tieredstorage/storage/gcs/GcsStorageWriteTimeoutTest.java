/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end proof that {@code gcs.http.write.timeout} bounds a stalled chunk-PUT upload. This
 * complements {@code MetricCollectorWriteTimeoutTest} — that test verifies the configuration is
 * plumbed into {@link com.google.api.client.http.HttpRequest}; this test verifies the timeout
 * actually FIRES on a real half-open TCP connection and surfaces as {@link StorageBackendException}.
 *
 * <p>The half-open-socket condition is reproduced by {@link HalfOpenGcsServer} — see that class
 * for the implementation. The test's job is to confirm:
 * <ol>
 *   <li>With {@code writeTimeout} set, the upload fails inside the configured timeout window
 *       (positive control).</li>
 *   <li>Without {@code writeTimeout}, the same upload does NOT complete within a short budget —
 *       proving the config is the load-bearing lever and not coincidental SDK behavior (negative
 *       control).</li>
 * </ol>
 *
 * <p>The class-level {@link Timeout} annotation is a hard guard: if any of these tests ever does
 * hang, JUnit kills it after the class timeout rather than letting the suite stall forever.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class GcsStorageWriteTimeoutTest {

    private static final int CHUNK_SIZE = 4 * 1024 * 1024;          // 4 MiB
    private static final int PAYLOAD_SIZE = 10 * 1024 * 1024;       // 10 MiB

    private HalfOpenGcsServer server;
    private GcsStorage storage;
    private ExecutorService backgroundUploader;

    @BeforeEach
    void setUp() throws Exception {
        server = new HalfOpenGcsServer();
        server.start();
        backgroundUploader = Executors.newSingleThreadExecutor(r -> {
            final Thread t = new Thread(r, "stalled-upload-test");
            t.setDaemon(true);
            return t;
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the server FIRST so any blocked client thread sees its socket close and
        // unblocks. Otherwise backgroundUploader.awaitTermination would wait its full budget
        // while the worker is parked in socketWrite0 with no way to be interrupted.
        if (server != null) {
            server.close();
        }
        if (backgroundUploader != null) {
            backgroundUploader.shutdownNow();
            backgroundUploader.awaitTermination(5, TimeUnit.SECONDS);
        }
        // Release the storage's resources (credentials watcher, metrics).
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void writeTimeoutFiresOnStalledChunkPut() throws Exception {
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.write.timeout", "1500",
            "gcs.api.retry.max.attempts", "1",
            "gcs.api.retry.total.timeout", "5000",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        final byte[] payload = new byte[PAYLOAD_SIZE];
        final long t0 = System.nanoTime();

        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(payload),
                                                new TestObjectKey("stalled-key")))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("Failed to upload");

        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        // Sanity: the stall actually engaged (we got past POST and into the chunk PUT).
        assertThat(server.awaitPut(Duration.ofSeconds(5)))
            .as("PUT was not received within 5s")
            .isTrue();

        // Lower bound: prove we actually waited for the timeout, not failed for an unrelated
        // reason. 33% margin under the 1.5s writeTimeout.
        assertThat(elapsedMs)
            .as("upload returned suspiciously early (%d ms); writeTimeout was 1500", elapsedMs)
            .isGreaterThanOrEqualTo(1_000);

        // Upper bound: 60s.
        //
        // Even with RetrySettings.maxAttempts=1 and totalTimeout=5000, the SDK's
        // resumable-upload retry path (JsonResumableSession + Retrying) does not fully honor
        // those bounds — the IOException from the writeTimeout is classified as retryable and
        // the SDK loops a handful of times with exponential backoff. The write timeout still
        // provides the critical guarantee (a bounded failure rather than an indefinite block),
        // but the effective upper bound is wider than naively writeTimeout * maxAttempts. 60s
        // is generous enough to be CI-stable while small enough that a regression that
        // re-introduces the unbounded block will fire it.
        assertThat(elapsedMs)
            .as("upload took %d ms; expected to fail well under 60s with writeTimeout=1500."
                + " If this fires close to the class-level @Timeout, the SDK's retry loop"
                + " has changed and the effective bound has loosened.", elapsedMs)
            .isLessThan(60_000);
    }

    @Test
    void operationTimeoutBoundsTotalUploadDuration() throws Exception {
        // The plugin-level hard wall. With this set, the total time spent inside
        // GcsStorage.upload() is bounded regardless of how many internal SDK retry layers loop.
        // Compare to writeTimeoutFiresOnStalledChunkPut (~25s observed) — with operationTimeout=3000
        // the same stalled upload should fail in ~3s.
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.write.timeout", "1500",
            "gcs.api.retry.max.attempts", "1",
            "gcs.operation.timeout", "3000",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        final byte[] payload = new byte[PAYLOAD_SIZE];
        final long t0 = System.nanoTime();

        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(payload),
                                                new TestObjectKey("stalled-key-3")))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContainingAll("Timed out", "PT3S", "upload");

        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertThat(server.awaitPut(Duration.ofSeconds(5)))
            .as("PUT was not received within 5s")
            .isTrue();

        // The whole point: this should fail near operationTimeout, well below the ~25s we'd see
        // without it. Generous 1.5s upper slack for executor scheduling and the cancel handshake.
        assertThat(elapsedMs)
            .as("upload should have timed out near 3000ms; observed %d ms", elapsedMs)
            .isBetween(2_500L, 5_000L);
    }

    @Test
    void withoutWriteTimeoutUploadHangsBeyondShortBudget() throws Exception {
        // Same fixture, but DO NOT set gcs.http.write.timeout. Cap SDK retries so the test can
        // prove the upload blocks, not that retries amplify it.
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.api.retry.max.attempts", "1",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        final byte[] payload = new byte[PAYLOAD_SIZE];

        // Run upload on a worker; assert it has NOT completed after 4s. Without writeTimeout,
        // NioSocketImpl.implWrite parks unbounded; without this guard the test would block until
        // the class's @Timeout fires (which would still be a passing assertion but a louder one).
        final Future<?> uploadFuture = backgroundUploader.submit(() ->
            storage.upload(new ByteArrayInputStream(payload), new TestObjectKey("stalled-key-2")));

        assertThatThrownBy(() -> uploadFuture.get(4, TimeUnit.SECONDS))
            .as("upload completed in <4s without writeTimeout — the stall did not engage,"
                + " or the SDK has built-in write bounding we didn't account for")
            .isInstanceOf(TimeoutException.class);

        assertThat(server.awaitPut(Duration.ofSeconds(1)))
            .as("PUT was not received before the 4s budget expired")
            .isTrue();

        // Fire-and-forget cancel. cancel(true) interrupts the worker but does not unblock
        // socketWrite0 — the worker will eventually unwind when tearDown closes the half-open
        // socket via server.close().
        uploadFuture.cancel(true);
    }
}
