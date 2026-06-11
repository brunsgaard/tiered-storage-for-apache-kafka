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

import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Characterizes how {@code gcs.http.read.timeout} actually behaves on the media-<em>download</em>
 * path (the lazily-read stream {@code fetch()} returns), which is the path {@code gcs.operation.timeout}
 * does NOT cover. This is a deliberate characterization test of a known limitation, not a "read is
 * bounded" proof — see {@code reference_gcs_readchannel_reopen} / the change record for context.
 *
 * <p>Findings encoded here (google-cloud-storage 2.61.0):
 * <ol>
 *   <li>The per-request read timeout IS applied to the download: each stalled read attempt fails at
 *       ~{@code read.timeout}, so within a short window we observe several attempts rather than one
 *       long block at the ~20s SDK default.</li>
 *   <li>BUT the {@code ReadChannel} reopens/retries the download on every timeout, unbounded by
 *       {@code gcs.api.retry.*} or {@code gcs.operation.timeout} — so {@code fetch()} does NOT
 *       fail fast on a persistent stall; the reading thread keeps looping.</li>
 * </ol>
 *
 * <p>{@link HalfOpenDownloadGcsServer} serves the object-metadata GET normally, then stalls the
 * {@code alt=media} body. The read runs on a background daemon (it would otherwise loop forever);
 * we observe the server's request log for repeated download attempts. If a future SDK upgrade
 * changes this behavior (e.g. bounds the reopen loop), this test trips and the read-path guidance
 * should be revisited. Run for both transports because production uses apache.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class GcsStorageReadDownloadTimeoutTest {

    private static final String BUCKET = "test-bucket";
    private static final long READ_TIMEOUT_MS = 1000;
    // Observe long enough for several per-attempt timeouts to elapse.
    private static final long OBSERVE_MS = 4 * READ_TIMEOUT_MS;

    private HalfOpenDownloadGcsServer server;
    private GcsStorage storage;

    @BeforeEach
    void setUp() throws Exception {
        server = new HalfOpenDownloadGcsServer(BUCKET);
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close the server first; that unblocks the still-looping background read so it unwinds.
        if (server != null) {
            server.close();
        }
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void downloadReopensPerReadTimeoutOnUrlconnection() throws Exception {
        runCharacterization(false);
    }

    @Test
    void downloadReopensPerReadTimeoutOnApache() throws Exception {
        runCharacterization(true);
    }

    private void runCharacterization(final boolean apache) throws Exception {
        final Map<String, Object> config = new HashMap<>();
        config.put("gcs.bucket.name", BUCKET);
        config.put("gcs.endpoint.url", server.url());
        config.put("gcs.credentials.default", "false");
        config.put("gcs.http.read.timeout", Long.toString(READ_TIMEOUT_MS));
        config.put("gcs.api.retry.max.attempts", "1");        // does NOT bound the reopen loop (the point)
        if (apache) {
            config.put("gcs.http.transport", "apache");
            config.put("gcs.operation.timeout", "120000");    // required for apache; covers metadata, not the stream
        }
        storage = new GcsStorage();
        storage.configure(config);

        // The read loops forever on a persistent stall, so run it on a daemon we can abandon; the
        // server close in tearDown breaks the loop.
        final Thread reader = new Thread(() -> {
            try (InputStream in = storage.fetch(new TestObjectKey("download-key"))) {
                in.readAllBytes();
            } catch (final Throwable ignored) {
                // expected once the server is closed during teardown
            }
        }, "characterization-reader");
        reader.setDaemon(true);
        reader.start();

        assertThat(server.awaitDownload(Duration.ofSeconds(5)))
            .as("media download was not attempted").isTrue();

        Thread.sleep(OBSERVE_MS);

        final long mediaAttempts = server.requestLines.stream()
            .filter(line -> line.contains("alt=media")).count();

        // (1) per-attempt timeout fires: with read.timeout=1s we see multiple attempts in ~4s,
        //     rather than a single attempt stuck on the ~20s SDK default.
        // (2) the reopen loop is unbounded by gcs.api.retry.* and gcs.operation.timeout: the read
        //     keeps re-issuing the download and the thread is still alive (not fast-failed/completed).
        assertThat(mediaAttempts)
            .as("transport=%s: with read.timeout=%dms a stalled download should be reopened "
                    + "multiple times in %dms (observed %d) — per-attempt timeout fires AND the SDK "
                    + "retries unboundedly", apache ? "apache" : "urlconnection",
                READ_TIMEOUT_MS, OBSERVE_MS, mediaAttempts)
            .isGreaterThanOrEqualTo(2);
        assertThat(reader.isAlive())
            .as("fetch() read should still be looping (read.timeout does not make the download fail "
                + "fast); if this is false the SDK now bounds the reopen loop and read-path guidance "
                + "should be revisited")
            .isTrue();
    }
}
