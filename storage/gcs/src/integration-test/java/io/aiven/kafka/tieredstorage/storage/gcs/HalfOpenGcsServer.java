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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A loopback HTTP stub that fakes just enough of the GCS resumable-upload protocol to
 * physically reproduce a half-open TCP write stall:
 *
 * <ol>
 *   <li>It accepts the resumable-upload init {@code POST}, returns {@code 200 OK} with a
 *       {@code Location} header pointing back at itself, then closes the socket so the chunk
 *       PUT runs on a fresh TCP connection.</li>
 *   <li>It accepts the chunk {@code PUT}, reads the request line + headers byte-by-byte until
 *       {@code \r\n\r\n}, then <strong>never reads the body and never responds</strong>. The
 *       kernel receive buffer fills, the TCP window falls to zero, the client's send buffer
 *       fills, and the client's {@code socketWrite0} blocks indefinitely — the half-open
 *       scenario this test targets.</li>
 * </ol>
 *
 * <p>The stub is intentionally small and dependency-free (no testcontainers, no Docker) so it
 * runs on any developer box. It is sufficient to validate that {@code gcs.http.write.timeout}
 * fires, but it is NOT a general-purpose GCS fake — it handles exactly the two requests above
 * and nothing else.
 *
 * <p><strong>Critical implementation notes</strong>:
 * <ul>
 *   <li>The wire is read with raw {@link InputStream#read()}, not via
 *       {@link java.io.BufferedReader} / {@link java.io.BufferedInputStream}. Buffered readers
 *       speculatively pull body bytes into user space and would defeat the stall by emptying
 *       the kernel buffer.</li>
 *   <li>The POST response sets {@code Connection: close} and the stub closes the socket
 *       physically after responding. This guarantees the PUT opens a new TCP connection with
 *       un-depleted {@code SO_SNDBUF}/{@code SO_RCVBUF}.</li>
 *   <li>The PUT handler parks via {@link LockSupport#park()} rather than
 *       {@link Thread#sleep(long) Thread.sleep(Long.MAX_VALUE)}. Both work, but {@code park()}
 *       unblocks immediately on {@code interrupt()}, simplifying teardown.</li>
 * </ul>
 */
final class HalfOpenGcsServer implements AutoCloseable {

    private static final Pattern REQUEST_LINE = Pattern.compile("^(\\S+) (\\S+) (\\S+)$");

    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final CopyOnWriteArrayList<Socket> liveSockets = new CopyOnWriteArrayList<>();
    private final CountDownLatch putReceived = new CountDownLatch(1);
    private final String uploadId = "upload-" + UUID.randomUUID();
    private final String url;

    HalfOpenGcsServer() throws IOException {
        // Bind to loopback only; port 0 lets the OS pick a free port for parallel-safety.
        this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.url = "http://127.0.0.1:" + serverSocket.getLocalPort();
        this.executor = Executors.newCachedThreadPool(r -> {
            final Thread t = new Thread(r, "half-open-gcs-server");
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

    /** Returns true if the chunk PUT was actually received before the deadline. */
    boolean awaitPut(final Duration deadline) throws InterruptedException {
        return putReceived.await(deadline.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        // 1. Close the listening socket; in-flight accept() throws SocketException and exits.
        try {
            serverSocket.close();
        } catch (final IOException ignored) {
            // best effort
        }
        // 2. Force-close every accepted socket; the parked PUT-handler thread sees
        //    an interrupt-or-close and unwinds.
        for (final Socket s : liveSockets) {
            try {
                s.close();
            } catch (final IOException ignored) {
                // best effort
            }
        }
        // 3. Interrupt any worker still parked or blocked in I/O, and assert it dies.
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("HalfOpenGcsServer worker(s) did not terminate");
        }
    }

    private void acceptLoop() {
        while (!serverSocket.isClosed()) {
            final Socket client;
            try {
                client = serverSocket.accept();
            } catch (final IOException e) {
                // expected when close() runs; otherwise propagating would just spam stderr
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
            // Drain remaining headers; we don't need them, but we MUST consume up to and
            // including the empty line, otherwise body bytes start landing in the parser.
            String line;
            while ((line = readLine(in)) != null && !line.isEmpty()) {
                // discard
            }

            final Matcher m = REQUEST_LINE.matcher(requestLine);
            if (!m.matches()) {
                return;
            }
            final String method = m.group(1);

            if ("POST".equals(method)) {
                respondWithLocation(out);
                // Close politely so the client opens a fresh connection for the PUT,
                // avoiding HTTP keepalive complications.
                client.close();
            } else if ("PUT".equals(method)) {
                putReceived.countDown();
                // Park forever. Body bytes accumulate in the kernel receive buffer until the
                // TCP window goes to zero; the client's writes block. close() interrupts this
                // thread and we exit.
                LockSupport.park();
            }
        } catch (final IOException e) {
            // socket was closed by tearDown; quiet exit
        }
    }

    private void respondWithLocation(final OutputStream out) throws IOException {
        final String location = url + "/upload/storage/v1/b/test-bucket/o"
            + "?uploadType=resumable&upload_id=" + uploadId;
        final String body = "{}";
        final String response = "HTTP/1.1 200 OK\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: " + body.length() + "\r\n"
            + "Location: " + location + "\r\n"
            + "Connection: close\r\n"
            + "\r\n"
            + body;
        out.write(response.getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    /**
     * Reads a single CRLF-terminated line from the stream byte-by-byte, returning the line
     * without its trailing CRLF. Returns null on EOF before any byte is read.
     *
     * <p>Buffered readers are intentionally not used: they speculatively pull body bytes into
     * a user-space buffer, draining the kernel buffer that we depend on filling up to block
     * the client's write side.
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
                // tolerate LF-only line endings
                return sb.toString();
            } else {
                sb.append((char) b);
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
