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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end proof that {@code gcs.http.read.timeout} bounds a stalled GCS read. This complements
 * {@code MetricCollectorTimeoutTest} (which verifies the value is plumbed into
 * {@link com.google.api.client.http.HttpRequest}); this test verifies the timeout actually FIRES
 * on a real half-open TCP connection and surfaces as {@link StorageBackendException}.
 *
 * <p>The stall is reproduced by {@link HalfOpenReadGcsServer}: it answers the GCS object-metadata
 * GET (issued by {@code fetch()} via {@code Storage.get}) with response headers promising a body it
 * never sends, so the client blocks reading. With {@code gcs.http.read.timeout} set the socket's
 * SO_TIMEOUT fires; without it, the read would block on the SDK default (~20s) or longer. This is
 * the read-path analogue of {@link GcsStorageWriteTimeoutTest}.
 *
 * <p>The class-level {@link Timeout} is a hard guard against a regression that re-introduces an
 * unbounded read.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class GcsStorageReadTimeoutTest {

    private HalfOpenReadGcsServer server;
    private GcsStorage storage;

    @BeforeEach
    void setUp() throws Exception {
        server = new HalfOpenReadGcsServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the server FIRST so the blocked client read sees the socket close and unwinds.
        if (server != null) {
            server.close();
        }
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void readTimeoutFiresOnStalledFetch() throws Exception {
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.read.timeout", "1500",
            "gcs.api.retry.max.attempts", "1"
        ));

        final long t0 = System.nanoTime();

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("stalled-key")))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("Failed to fetch");

        final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        // Sanity: the stall actually engaged (the GET reached the server).
        assertThat(server.awaitRequest(Duration.ofSeconds(5)))
            .as("GET was not received within 5s")
            .isTrue();

        // Lower bound: prove we waited for the read timeout rather than failing for an unrelated
        // reason (e.g. connection refused). 33% margin under the 1.5s readTimeout.
        assertThat(elapsedMs)
            .as("fetch returned suspiciously early (%d ms); readTimeout was 1500", elapsedMs)
            .isGreaterThanOrEqualTo(1_000);

        // Upper bound: 60s. The SDK may classify the SocketTimeoutException as retryable and loop a
        // few times, so the effective bound is wider than readTimeout * maxAttempts; 60s is CI-stable
        // yet small enough that a regression re-introducing the ~20s-default (or unbounded) read fires
        // it. The load-bearing guarantee is a bounded failure, not an indefinite block.
        assertThat(elapsedMs)
            .as("fetch took %d ms; expected to fail well under 60s with readTimeout=1500", elapsedMs)
            .isLessThan(60_000);
    }
}
