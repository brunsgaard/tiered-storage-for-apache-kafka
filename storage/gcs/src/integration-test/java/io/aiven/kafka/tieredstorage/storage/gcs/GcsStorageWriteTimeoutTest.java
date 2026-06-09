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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
            "gcs.http.transport", "apache",   // gcs.operation.timeout requires the abortable transport
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

    /**
     * Detect whether a worker thread that ran a cancelled upload is still alive and parked in a
     * blocking socket syscall. Returns the names of leaked threads (empty if none). Looks for
     * threads named "gcs-operation*" (set by GcsStorage's executor factory) not present in the
     * baseline, whose stack contains a write- or read-side blocking I/O frame.
     *
     * <p>The stall blocks in {@code socketWrite0}. On some hosts the worker finishes the chunk PUT
     * body before the call timeout fires and ends up parked reading the response from the
     * half-open server (parking in {@code Net.poll} because gcs.http.write.timeout sets a
     * SO_TIMEOUT on the socket). Either way it's the same daemon-thread leak that
     * {@code Future.cancel(true)} cannot unblock.
     */
    private List<String> findLeakedApiCallWorkers(final Set<Long> baselineThreadIds) {
        final Map<Thread, StackTraceElement[]> all = Thread.getAllStackTraces();
        return all.entrySet().stream()
            .filter(e -> e.getKey().getName().startsWith("gcs-operation"))
            .filter(e -> !baselineThreadIds.contains(e.getKey().getId()))
            .filter(e -> Arrays.stream(e.getValue()).anyMatch(GcsStorageWriteTimeoutTest::isBlockingSocketFrame))
            .map(e -> e.getKey().getName() + " @ " + e.getValue()[0])
            .collect(Collectors.toList());
    }

    private static boolean isBlockingSocketFrame(final StackTraceElement f) {
        final String cls = f.getClassName();
        final String m = f.getMethodName();
        // Write-side leak (the original case): worker parked in the kernel write path before the
        // response is even attempted.
        if ("implWrite".equals(m) || "socketWrite0".equals(m) || "writeContentToOutputStream".equals(m)) {
            return true;
        }
        // Read-side leak: the chunk PUT body flushed before the call timeout fired and the worker
        // is parked reading the response from the half-open server.
        if ("implRead".equals(m) || "socketRead0".equals(m)) {
            return true;
        }
        // Lowest-level park frame used by NioSocketImpl when SO_TIMEOUT is set. Catches the case
        // where upper-stack method names changed between JDK versions.
        return "sun.nio.ch.Net".equals(cls) && "poll".equals(m);
    }

    private static Set<Long> snapshotApiCallThreadIds() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.getName().startsWith("gcs-operation"))
            .map(Thread::getId)
            .collect(Collectors.toSet());
    }

    @Test
    void apacheTransportDoesNotLeakWorkerOnApiCallTimeout() throws Exception {
        // With gcs.http.transport=apache and the abort-on-timeout plumbing in runBounded: when the
        // call timeout fires, the canceller calls HttpRequestBase.abort() on the worker's in-flight
        // request, which shuts down the Apache connection, closes the socket, and propagates to the
        // parked socketWrite0 as IOException so the worker completes and exits. No leak.
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.transport", "apache",
            "gcs.http.write.timeout", "60000",
            "gcs.operation.timeout", "1500",
            "gcs.api.retry.max.attempts", "1",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        final Set<Long> baseline = snapshotApiCallThreadIds();

        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(new byte[PAYLOAD_SIZE]),
                                                new TestObjectKey("leak-apache")))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("Timed out");

        assertThat(server.awaitPut(Duration.ofSeconds(5))).isTrue();

        // Poll for up to 5s for the leak to clear. If abort() unblocks the worker promptly, this
        // completes in <100 ms. If the leak persists the full 5s, abort isn't reaching the
        // in-flight write — dump diagnostics and fail.
        List<String> leaked = findLeakedApiCallWorkers(baseline);
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!leaked.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(50);
            leaked = findLeakedApiCallWorkers(baseline);
        }

        if (!leaked.isEmpty()) {
            final StringBuilder dump = new StringBuilder();
            Thread.getAllStackTraces().entrySet().stream()
                .filter(e -> e.getKey().getName().startsWith("gcs-operation"))
                .forEach(e -> {
                    dump.append("\n").append(e.getKey().getName()).append("\n");
                    Arrays.stream(e.getValue()).limit(60)
                        .forEach(f -> dump.append("    at ").append(f).append("\n"));
                });
            System.err.println("[apacheTransportDoesNotLeakWorkerOnApiCallTimeout] "
                + "leak persisted past 5s. Full worker stacks:" + dump);
        }

        assertThat(leaked)
            .as("apache transport with abort-on-timeout: expected NO gcs-operation worker still"
                + " parked in a write syscall after the call timeout. If this fails, the abort"
                + " plumbing in GcsStorage.runBounded is not reaching the in-flight HttpRequestBase.")
            .isEmpty();
    }

    @Test
    void interruptedCallerAbortsWorkerNoLeak() throws Exception {
        // The interrupt path of runBounded must abort the in-flight request just like the timeout
        // path. Interruption of the calling thread is routine during RLM task cancellation
        // (partition reassignment, broker shutdown). Here operationTimeout is long (60s) so it never
        // fires — the ONLY thing that unblocks the call is the interrupt — and we assert the inner
        // gcs-operation worker doesn't leak.
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.transport", "apache",
            "gcs.http.write.timeout", "60000",
            "gcs.operation.timeout", "60000",         // long — interrupt, not timeout, ends the call
            "gcs.api.retry.max.attempts", "1",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        final Set<Long> baseline = snapshotApiCallThreadIds();

        // Run the upload on a worker we can interrupt. The caller blocks in runBounded's
        // future.get(); cancel(true) interrupts THAT caller thread, surfacing InterruptedException
        // inside runBounded (not the inner gcs-operation worker, which is parked in socketWrite0).
        final Future<?> caller = backgroundUploader.submit(() ->
            storage.upload(new ByteArrayInputStream(new byte[PAYLOAD_SIZE]),
                           new TestObjectKey("interrupted-caller")));

        assertThat(server.awaitPut(Duration.ofSeconds(5)))
            .as("inner worker did not reach the chunk PUT within 5s")
            .isTrue();

        // Interrupt the calling thread → runBounded hits catch(InterruptedException) → must abort
        // the inner worker's request.
        caller.cancel(true);

        // The inner gcs-operation worker should unwind promptly once its request is aborted.
        List<String> leaked = findLeakedApiCallWorkers(baseline);
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!leaked.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(50);
            leaked = findLeakedApiCallWorkers(baseline);
        }
        assertThat(leaked)
            .as("interrupt path must abort the in-flight request; found leaked worker(s) %s", leaked)
            .isEmpty();
    }

    @Test
    void cancelledWorkersClearedSoSubsequentUploadIsNotPreAborted() throws Exception {
        // Regression guard: if cancelledWorkers is not cleared in the runBounded lambda's finally,
        // a subsequent task that lands on the SAME daemon-pool worker (cached executor reuses
        // threads) gets pre-aborted by the request interceptor — manifesting as random fast
        // failures on healthy uploads after any timeout event.
        //
        // We trigger two consecutive timeouts on the same GcsStorage. Both target the half-open
        // stub (so both DO time out), but the second one must take ~operationTimeout — NOT <500ms
        // (which would mean it was pre-aborted on entry). Same logic exercises the
        // activeApacheRequests cleanup.
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.transport", "apache",
            "gcs.http.write.timeout", "60000",
            "gcs.operation.timeout", "1500",
            "gcs.api.retry.max.attempts", "1",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        // First upload — stalls, gets cancelled at operationTimeout=1500ms.
        final long t0 = System.nanoTime();
        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(new byte[PAYLOAD_SIZE]),
                                                new TestObjectKey("first")))
            .isInstanceOf(StorageBackendException.class);
        final long firstElapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertThat(firstElapsed).as("first upload should have waited near operationTimeout")
            .isBetween(1_000L, 5_000L);

        // Second upload — re-runs through the same GcsStorage and (likely) the same worker thread
        // from the cached pool. If state-machine cleanup is correct, it ALSO waits ~operationTimeout.
        // If cleanup is broken, the interceptor sees the worker still in cancelledWorkers and
        // pre-aborts the request, which would short-circuit to ~ms.
        final long t1 = System.nanoTime();
        assertThatThrownBy(() -> storage.upload(new ByteArrayInputStream(new byte[PAYLOAD_SIZE]),
                                                new TestObjectKey("second")))
            .isInstanceOf(StorageBackendException.class);
        final long secondElapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);

        assertThat(secondElapsed)
            .as("second upload finished suspiciously fast (%d ms); likely the previous timeout left"
                + " state in cancelledWorkers/activeApacheRequests and the request interceptor"
                + " pre-aborted on entry. Check the lambda's finally block in runBounded.",
                secondElapsed)
            .isGreaterThanOrEqualTo(1_000L);
        assertThat(secondElapsed).isLessThan(5_000L);
    }

    @Test
    void concurrentUploadsAllBoundedAndCleanedUp() throws Exception {
        // Exercises the ConcurrentMap-backed state machine (activeApacheRequests, cancelledWorkers)
        // under realistic concurrency: five simultaneous uploads, all stalled, all bounded by
        // operationTimeout. Every one must throw StorageBackendException, and after the run there
        // must be no leaked gcs-operation workers.
        final int parallelism = 5;
        storage = new GcsStorage();
        storage.configure(Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", server.url(),
            "gcs.credentials.default", "false",
            "gcs.http.transport", "apache",
            "gcs.http.write.timeout", "60000",
            "gcs.operation.timeout", "1500",
            "gcs.api.retry.max.attempts", "1",
            "gcs.resumable.upload.chunk.size", Integer.toString(CHUNK_SIZE)
        ));

        final Set<Long> baseline = snapshotApiCallThreadIds();
        final ExecutorService driver = Executors.newFixedThreadPool(parallelism, r -> {
            final Thread t = new Thread(r, "concurrent-test-driver");
            t.setDaemon(true);
            return t;
        });
        try {
            final long t0 = System.nanoTime();
            final List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                final int idx = i;
                futures.add(driver.submit(() ->
                    storage.upload(new ByteArrayInputStream(new byte[PAYLOAD_SIZE]),
                                   new TestObjectKey("concurrent-" + idx))));
            }
            // Each upload completes with StorageBackendException; the driver executor surfaces it
            // via ExecutionException. All 5 completing within a bounded window proves they ran
            // concurrently (not serialized) and the state machine handled overlapping
            // cancellations correctly.
            for (final Future<?> f : futures) {
                assertThatThrownBy(() -> f.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasCauseInstanceOf(StorageBackendException.class);
            }
            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

            // Five concurrent uploads each bounded at 1500ms should finish under ~5s. Serialized,
            // we'd see ~7.5s; a deadlocked pair would never reach here (Future.get(10s) throws).
            assertThat(elapsedMs)
                .as("5 concurrent uploads took %d ms; expected to complete in parallel,"
                    + " not serialized", elapsedMs)
                .isLessThan(5_000L);

            // Brief settle, then verify no gcs-operation workers are still parked in a write syscall.
            Thread.sleep(500);
            final List<String> leaked = findLeakedApiCallWorkers(baseline);
            assertThat(leaked)
                .as("Found %d leaked workers after concurrent stress; the state machine under load"
                    + " is leaking. Inspect activeApacheRequests/cancelledWorkers cleanup.",
                    leaked.size())
                .isEmpty();
        } finally {
            driver.shutdownNow();
            driver.awaitTermination(5, TimeUnit.SECONDS);
        }
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
