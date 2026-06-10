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
 * A loopback HTTP stub that reproduces a half-open TCP <em>read</em> stall: it accepts a request,
 * sends a {@code 200 OK} status line and headers advertising a large {@code Content-Length}, then
 * <strong>never sends the body and never closes the socket</strong>. The client blocks reading the
 * response body; with {@code gcs.http.read.timeout} set, the socket's {@code SO_TIMEOUT} fires and
 * the read fails with {@link java.net.SocketTimeoutException} instead of blocking until the kernel
 * tears the connection down.
 *
 * <p>This is the read-path counterpart to {@link HalfOpenGcsServer}. The plugin issues the GCS
 * object-metadata GET first ({@code Storage.get}); stalling that single GET is sufficient to prove
 * the read timeout reaches every request through the configured transport — the same SO_TIMEOUT
 * also governs the streaming media download that backs {@code fetch()}'s lazily-read stream.
 *
 * <p>Like {@link HalfOpenGcsServer}, it is dependency-free (no testcontainers/Docker) and handles
 * exactly this one behaviour. The response headers are written, then the handler parks via
 * {@link LockSupport#park()} so it unblocks promptly on {@code interrupt()} during teardown.
 */
final class HalfOpenReadGcsServer implements AutoCloseable {

    // Advertised but never-delivered body size. Large enough that the client must block on a read.
    private static final int ADVERTISED_BODY_BYTES = 8 * 1024 * 1024;

    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final CopyOnWriteArrayList<Socket> liveSockets = new CopyOnWriteArrayList<>();
    private final CountDownLatch requestReceived = new CountDownLatch(1);
    private final String url;

    HalfOpenReadGcsServer() throws IOException {
        // Bind to loopback only; port 0 lets the OS pick a free port for parallel-safety.
        this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.url = "http://127.0.0.1:" + serverSocket.getLocalPort();
        this.executor = Executors.newCachedThreadPool(r -> {
            final Thread t = new Thread(r, "half-open-read-gcs-server");
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

    /** Returns true if a request was actually received before the deadline. */
    boolean awaitRequest(final Duration deadline) throws InterruptedException {
        return requestReceived.await(deadline.toMillis(), TimeUnit.MILLISECONDS);
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
            throw new IllegalStateException("HalfOpenReadGcsServer worker(s) did not terminate");
        }
    }

    private void acceptLoop() {
        while (!serverSocket.isClosed()) {
            final Socket client;
            try {
                client = serverSocket.accept();
            } catch (final IOException e) {
                // expected when close() runs
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

            // Consume the request line + headers up to and including the empty line.
            String line = readLine(in);
            if (line == null) {
                return;
            }
            while ((line = readLine(in)) != null && !line.isEmpty()) {
                // discard
            }

            requestReceived.countDown();

            // Send a complete response head that promises a body, then send nothing. The client
            // blocks reading the body; SO_TIMEOUT (gcs.http.read.timeout) is what unblocks it.
            final String head = "HTTP/1.1 200 OK\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: " + ADVERTISED_BODY_BYTES + "\r\n"
                + "\r\n";
            out.write(head.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            // Park forever; teardown interrupts us.
            LockSupport.park();
        } catch (final IOException e) {
            // socket closed by teardown; quiet exit
        }
    }

    /**
     * Reads a single CRLF-terminated line byte-by-byte (no buffered reader, to avoid pulling
     * bytes we don't intend to consume). Returns the line without its trailing CRLF, or null on
     * EOF before any byte is read.
     */
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
