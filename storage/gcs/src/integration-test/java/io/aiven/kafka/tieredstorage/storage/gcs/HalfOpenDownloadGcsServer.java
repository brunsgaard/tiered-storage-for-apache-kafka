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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A loopback HTTP stub that reproduces a half-open stall on the GCS media <em>download</em> — the
 * lazily-read body behind {@code fetch()} — while answering the preceding object-metadata GET
 * normally. This isolates the streaming read: the metadata {@code Storage.get} succeeds, the
 * {@code ReadChannel} download then blocks because the body never arrives.
 *
 * <ul>
 *   <li>A request whose target contains {@code alt=media} (or starts with {@code /download/}) is the
 *       media download: respond {@code 200} with a large {@code Content-Length}, send no body, and
 *       park. The client blocks reading the body; only the socket read timeout
 *       ({@code gcs.http.read.timeout}) can unblock it.</li>
 *   <li>Any other GET is treated as the object-metadata fetch: respond {@code 200} with a complete
 *       minimal {@code storage#object} JSON so {@code Storage.get} returns a non-null Blob, then
 *       close the connection so the download opens fresh.</li>
 * </ul>
 *
 * <p>This exists specifically to validate that the SDK honors the per-request read timeout on the
 * media-download path (the assumption that the read-timeout fix rests on). Dependency-free, like
 * {@link HalfOpenGcsServer}.
 */
final class HalfOpenDownloadGcsServer implements AutoCloseable {

    private static final int ADVERTISED_BODY_BYTES = 8 * 1024 * 1024;

    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final CopyOnWriteArrayList<Socket> liveSockets = new CopyOnWriteArrayList<>();
    private final CountDownLatch downloadReceived = new CountDownLatch(1);
    final CopyOnWriteArrayList<String> requestLines = new CopyOnWriteArrayList<>();
    private final String bucket;
    private final String url;

    HalfOpenDownloadGcsServer(final String bucket) throws IOException {
        this.bucket = bucket;
        this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.url = "http://127.0.0.1:" + serverSocket.getLocalPort();
        this.executor = Executors.newCachedThreadPool(r -> {
            final Thread t = new Thread(r, "half-open-download-gcs-server");
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        executor.submit(this::acceptLoop);
    }

    String url() {
        return url;
    }

    /** Returns true if the media download request was received before the deadline. */
    boolean awaitDownload(final Duration deadline) throws InterruptedException {
        return downloadReceived.await(deadline.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        try {
            serverSocket.close();
        } catch (final IOException ignored) {
            // best effort
        }
        for (final Socket s : liveSockets) {
            try {
                s.close();
            } catch (final IOException ignored) {
                // best effort
            }
        }
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("HalfOpenDownloadGcsServer worker(s) did not terminate");
        }
    }

    private void acceptLoop() {
        while (!serverSocket.isClosed()) {
            final Socket client;
            try {
                client = serverSocket.accept();
            } catch (final IOException e) {
                return;
            }
            liveSockets.add(client);
            executor.submit(() -> handle(client));
        }
    }

    private void handle(final Socket client) {
        try {
            final InputStream in = client.getInputStream();
            final OutputStream out = client.getOutputStream();

            final String requestLine = readLine(in);
            if (requestLine == null) {
                return;
            }
            requestLines.add(requestLine);
            String line;
            while ((line = readLine(in)) != null && !line.isEmpty()) {
                // drain headers
            }

            final boolean isMedia = requestLine.contains("alt=media") || requestLine.contains("/download/");
            if (isMedia) {
                downloadReceived.countDown();
                final String head = "HTTP/1.1 200 OK\r\n"
                    + "Content-Type: application/octet-stream\r\n"
                    + "Content-Length: " + ADVERTISED_BODY_BYTES + "\r\n"
                    + "\r\n";
                out.write(head.getBytes(StandardCharsets.US_ASCII));
                out.flush();
                // Never send the body; the client blocks reading it. Park until teardown.
                LockSupport.park();
            } else {
                respondWithMetadata(out, objectNameFrom(requestLine));
                client.close();
            }
        } catch (final IOException e) {
            // socket closed by teardown
        }
    }

    private void respondWithMetadata(final OutputStream out, final String objectName) throws IOException {
        final String body = "{"
            + "\"kind\":\"storage#object\","
            + "\"bucket\":\"" + bucket + "\","
            + "\"name\":\"" + objectName + "\","
            + "\"generation\":\"1\","
            + "\"metageneration\":\"1\","
            + "\"contentType\":\"application/octet-stream\","
            + "\"size\":\"" + ADVERTISED_BODY_BYTES + "\","
            + "\"timeCreated\":\"2026-01-01T00:00:00.000Z\","
            + "\"updated\":\"2026-01-01T00:00:00.000Z\""
            + "}";
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        final String head = "HTTP/1.1 200 OK\r\n"
            + "Content-Type: application/json; charset=UTF-8\r\n"
            + "Content-Length: " + bytes.length + "\r\n"
            + "Connection: close\r\n"
            + "\r\n";
        out.write(head.getBytes(StandardCharsets.US_ASCII));
        out.write(bytes);
        out.flush();
    }

    /** Extract the object name from {@code GET /.../o/<name>[?query] HTTP/1.1}. */
    private static String objectNameFrom(final String requestLine) {
        final String[] parts = requestLine.split(" ");
        if (parts.length < 2) {
            return "unknown";
        }
        String target = parts[1];
        final int q = target.indexOf('?');
        if (q >= 0) {
            target = target.substring(0, q);
        }
        final int o = target.lastIndexOf("/o/");
        return o >= 0 ? target.substring(o + 3) : target;
    }

    private static String readLine(final InputStream in) throws IOException {
        final StringBuilder sb = new StringBuilder(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                final int next = in.read();
                if (next == '\n' || next == -1) {
                    return sb.toString();
                }
                sb.append((char) b);
                sb.append((char) next);
            } else if (b == '\n') {
                return sb.toString();
            } else {
                sb.append((char) b);
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
