/*
 * Copyright 2023 Aiven Oy
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
import java.nio.channels.Channels;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;
import io.aiven.kafka.tieredstorage.storage.proxy.Socks5ProxyAuthenticator;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.http.impl.client.HttpClientBuilder;

public class GcsStorage implements StorageBackend {
    // Apache HttpClient connection-pool sizing (total and per-route). PoolingHttpClientConnectionManager
    // defaults to 20 total / 2 per route, which would serialize concurrent upload/fetch/delete behind
    // 2 connections to storage.googleapis.com; we bump both well above peak GCS concurrency and leave
    // slack for in-flight aborts (a connection mid-shutdown doesn't free its slot until close completes).
    private static final int MAX_HTTP_CONNECTIONS = 50;

    private volatile Storage storage;
    private String bucketName;
    private MetricCollector metricCollector;
    private Integer resumableUploadChunkSize;
    private ReloadableCredentialsProvider credentialsProvider;
    private StorageOptions.Builder storageOptionsBuilder;
    private Duration operationTimeout;
    // Cached/unbounded: its live thread count is naturally bounded by the broker's RemoteLogManager
    // pools (copier/reader/expiration), which are the threads that call into runBounded — we don't
    // impose a second, arbitrary cap. Package-private so the lifecycle test can observe shutdown.
    ExecutorService operationExecutor;

    // The Apache HttpClient transport, when gcs.http.transport=apache. Built once in configure()
    // and reused across every Storage client rebuild (the transport is credential-independent, so
    // it's safe to share). The transport factory below returns this instance rather than minting a
    // new one per build — otherwise each credentials reload (updateStorageClient) would spawn a new
    // connection pool that is never shut down, since Storage is not AutoCloseable. Shut down in close().
    private ApacheHttpTransport apacheTransport;

    // Tracks in-flight Apache requests per worker thread so a bounded call can be force-aborted on
    // timeout/interruption (see AbortableRequestTracker). Always allocated (cheap); its interceptor
    // is only wired in when transport=apache, and config validation guarantees that implies
    // operationTimeout is set, so tracking is always paired with the bounded path that cleans it up.
    private final AbortableRequestTracker requestTracker = new AbortableRequestTracker();

    @Override
    public void configure(final Map<String, ?> configs) {
        // configure() is normally called once, but be idempotent: release anything a prior call
        // created so a re-configure doesn't leak the executor/credentials watcher or orphan a
        // Metrics instance whose JmxReporter still owns the (JVM-global, untagged) GCS MBeans.
        releaseResourcesQuietly();

        final GcsStorageConfig config = new GcsStorageConfig(configs);
        this.bucketName = config.bucketName();

        final HttpTransportOptions.Builder httpTransportOptionsBuilder = HttpTransportOptions.newBuilder();

        final ProxyConfig proxyConfig = config.proxyConfig();
        if (proxyConfig != null) {
            httpTransportOptionsBuilder.setHttpTransportFactory(
                new ProxiedHttpTransportFactory(proxyConfig.host(), proxyConfig.port())
            );
            if (proxyConfig.username() != null) {
                Socks5ProxyAuthenticator.register(
                    proxyConfig.host(), proxyConfig.port(), proxyConfig.username(), proxyConfig.password());
            }
        } else if (GcsStorageConfig.GCS_HTTP_TRANSPORT_APACHE.equals(config.httpTransport())) {
            // Apache HttpClient transport, chosen over the default NetHttpTransport because the
            // underlying connection can be force-closed via HttpUriRequest.abort() — the lever the
            // plugin uses on operation-timeout to unblock a worker parked in socketWrite0. The
            // requestTracker's interceptor registers each in-flight request keyed by the worker
            // thread that issued it; when operation-timeout fires, runBounded looks the request up by
            // thread and aborts it, which shuts down the connection and propagates to the parked
            // write as an IOException so the worker unwinds cleanly.
            final var httpClient = HttpClientBuilder.create()
                .useSystemProperties()
                .addInterceptorFirst(requestTracker.interceptor())
                // Disable Apache's automatic retry layer (RetryExec). It would catch the
                // SocketException thrown when we abort() and silently re-issue the request on a
                // fresh connection, defeating the abort. The GCS SDK has its own retry layer
                // (controlled via gcs.api.retry.*), so Apache's is redundant and harmful here.
                .disableAutomaticRetries()
                .setMaxConnTotal(MAX_HTTP_CONNECTIONS)
                .setMaxConnPerRoute(MAX_HTTP_CONNECTIONS)
                .build();
            // Build the transport once and reuse it for every client rebuild (see field comment).
            this.apacheTransport = new ApacheHttpTransport(httpClient);
            httpTransportOptionsBuilder.setHttpTransportFactory(() -> apacheTransport);
        }

        metricCollector = new MetricCollector();

        // Create reloadable credentials provider
        this.credentialsProvider = config.reloadableCredentials();

        // Store the builder template for recreating storage clients. The HTTP timeouts and
        // retry settings are set on the template (not on the per-call client) so they are
        // preserved when the client is rebuilt on a credentials reload.
        this.storageOptionsBuilder = StorageOptions.newBuilder()
            .setTransportOptions(metricCollector.httpTransportOptions(
                httpTransportOptionsBuilder,
                config.httpWriteTimeout(),
                config.httpReadTimeout(),
                config.httpConnectTimeout()));
        if (config.endpointUrl() != null) {
            this.storageOptionsBuilder.setHost(config.endpointUrl());
        }

        final RetrySettings retrySettings = buildRetrySettings(
            config.apiRetryTotalTimeout(),
            config.apiRetryMaxAttempts());
        if (retrySettings != null) {
            this.storageOptionsBuilder.setRetrySettings(retrySettings);
        }

        // Set up credentials reload callback to recreate storage client
        this.credentialsProvider.setCredentialsUpdateCallback(this::updateStorageClient);

        // Create initial storage client
        updateStorageClient(credentialsProvider.getCredentials());

        resumableUploadChunkSize = config.resumableUploadChunkSize();

        // Plugin-level hard timeout for upload/fetch/delete. When set, calls run on a
        // daemon-threaded executor and the calling thread is bounded by Future.get(). This is
        // the outer wall around the SDK's internal retry layers, which empirically do not always
        // honor RetrySettings.totalTimeout for resumable uploads.
        operationTimeout = config.operationTimeout();
        if (operationTimeout != null) {
            operationExecutor = newOperationExecutor();
        }
    }

    /**
     * Build a {@link RetrySettings} on top of the SDK defaults, overriding only the fields the
     * user actually configured. Returns {@code null} when neither override is set, so the SDK
     * default is left untouched.
     *
     * <p>Package-private (rather than private) so {@code GcsStorageRetrySettingsTest} can verify
     * the override semantics directly; the one-line {@code setRetrySettings} call site in
     * {@link #configure} is too trivial to test independently.
     */
    static RetrySettings buildRetrySettings(final Duration totalTimeout, final Integer maxAttempts) {
        if (totalTimeout == null && maxAttempts == null) {
            return null;
        }
        final RetrySettings.Builder retryBuilder = StorageOptions.getDefaultRetrySettings().toBuilder();
        if (totalTimeout != null) {
            retryBuilder.setTotalTimeoutDuration(totalTimeout);
        }
        if (maxAttempts != null) {
            retryBuilder.setMaxAttempts(maxAttempts);
        }
        return retryBuilder.build();
    }

    /**
     * A cached daemon thread pool for {@link #runBounded} tasks. It is intentionally unbounded: the
     * number of live threads tracks the number of in-flight calls, which is itself bounded by the
     * broker's RemoteLogManager pools (the threads that call into {@code runBounded}) — the
     * authoritative concurrency limit, which we do not second-guess with an arbitrary cap. Because
     * {@code gcs.operation.timeout} requires the apache transport, a timed-out worker is always
     * aborted and freed, so threads do not accumulate beyond that natural bound.
     *
     * <p>Threads are daemon and uniquely named ({@code gcs-operation-0}, {@code -1}, …) so they are
     * distinguishable in thread dumps. Package-private so {@code GcsStorageOperationExecutorTest} can
     * assert the naming.
     */
    static ExecutorService newOperationExecutor() {
        final AtomicInteger threadNumber = new AtomicInteger();
        final ThreadFactory threadFactory = r -> {
            final Thread t = new Thread(r, "gcs-operation-" + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        return Executors.newCachedThreadPool(threadFactory);
    }

    /**
     * Best-effort release of everything {@link #configure} allocates, swallowing errors. Called at
     * the top of {@code configure()} to make re-configuration idempotent; {@link #close()} performs
     * the same teardown but surfaces failures to the caller. Fields are nulled so a subsequent
     * {@code configure()} starts clean (e.g. switching apache -> urlconnection drops the transport).
     */
    private void releaseResourcesQuietly() {
        // Shut the transport down before the executor (same NIO-interrupt rationale as close()):
        // closing sockets unwinds blocked workers, so the subsequent shutdownNow() interrupt is
        // harmless rather than a potential park of this (configure()) thread.
        if (apacheTransport != null) {
            try {
                apacheTransport.shutdown();
            } catch (final IOException ignored) {
                // best effort
            }
            apacheTransport = null;
        }
        if (operationExecutor != null) {
            operationExecutor.shutdownNow();
            operationExecutor = null;
        }
        if (credentialsProvider != null) {
            try {
                credentialsProvider.close();
            } catch (final Exception ignored) {
                // best effort
            }
            credentialsProvider = null;
        }
        if (metricCollector != null) {
            try {
                metricCollector.close();
            } catch (final IOException ignored) {
                // best effort
            }
            metricCollector = null;
        }
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        return runBounded(() -> doUpload(inputStream, key), "upload " + key);
    }

    private long doUpload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        // Note: when this runs on the operation executor and gcs.operation.timeout fires on the
        // non-abortable urlconnection transport, the caller (runBounded) returns while this worker
        // may still be reading inputStream. That is benign here — the worker's result is discarded
        // and the SDK closes the stream when its createFrom unwinds — but it is why the apache
        // transport (which aborts the request) is preferred when a timeout is configured.
        try {
            final BlobInfo blobInfo = BlobInfo.newBuilder(this.bucketName, key.value()).build();
            final Blob blob;
            if (resumableUploadChunkSize != null) {
                blob = storage.createFrom(blobInfo, inputStream, resumableUploadChunkSize);
            } else {
                blob = storage.createFrom(blobInfo, inputStream);
            }
            return blob.getSize();
        } catch (final IOException | BaseServiceException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        runBounded(() -> {
            doDelete(key);
            return null;
        }, "delete " + key);
    }

    private void doDelete(final ObjectKey key) throws StorageBackendException {
        try {
            storage.delete(this.bucketName, key.value());
        } catch (final BaseServiceException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        return runBounded(() -> doFetch(key), "fetch " + key);
    }

    private InputStream doFetch(final ObjectKey key) throws StorageBackendException {
        try {
            final Blob blob = getBlob(key);
            final ReadChannel reader = blob.reader();
            return Channels.newInputStream(reader);
        } catch (final BaseServiceException e) {
            if (e.getCode() == 404) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#404_Not_Found
                throw new KeyNotFoundException(this, key, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        return runBounded(() -> doFetchRange(key, range), "fetch " + key + " range=" + range);
    }

    private InputStream doFetchRange(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        try {
            if (range.isEmpty()) {
                return InputStream.nullInputStream();
            }

            final Blob blob = getBlob(key);

            if (range.firstPosition() >= blob.getSize()) {
                throw new InvalidRangeException("Range start position " + range.firstPosition()
                    + " is outside file content. file size = " + blob.getSize());
            }

            final ReadChannel reader = blob.reader();
            reader.limit(range.lastPosition() + 1);
            reader.seek(range.firstPosition());
            return Channels.newInputStream(reader);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final BaseServiceException e) {
            if (e.getCode() == 404) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#404_Not_Found
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getCode() == 416) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#416_Requested_Range_Not_Satisfiable
                throw new InvalidRangeException("Invalid range " + range, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    /**
     * Bounded wrapper around an SDK call. When {@link #operationTimeout} is null, runs the action
     * synchronously on the calling thread (no overhead, no behavior change). When set, runs the
     * action on a daemon-threaded executor and bounds the wait with
     * {@link Future#get(long, TimeUnit)}.
     *
     * <p>On {@link TimeoutException}, throws {@link StorageBackendException} immediately and
     * {@linkplain #cancelWorker cancels the worker}. With {@code gcs.http.transport=apache} the
     * worker's in-flight request is aborted, so it unblocks promptly; with the default
     * {@code urlconnection} transport the worker keeps running (because {@code socketWrite0} is a
     * native blocking call that does not honor {@link Thread#interrupt()}) and completes only when
     * the kernel TCP retransmit window expires.
     *
     * <p>On {@link ExecutionException}, unwraps and rethrows the underlying
     * {@link StorageBackendException} (or its subclasses {@link KeyNotFoundException} /
     * {@link InvalidRangeException}) so the call's exception contract is preserved.
     */
    private <T> T runBounded(final Callable<T> action, final String description)
        throws StorageBackendException {
        if (operationExecutor == null) {
            return runDirectly(action, description);
        }
        // Capture the worker thread so the canceller can look up its in-flight Apache request on
        // timeout. Set inside the lambda (so it's the actual worker, not the submitter), cleared
        // in finally so activeApacheRequests/cancelledWorkers don't grow.
        final AtomicReference<Thread> workerRef = new AtomicReference<>();
        // Guards the race where the timeout/interrupt handler fires before the worker has published
        // its thread into workerRef: there, cancelWorker() sees a null worker and cannot abort the
        // in-flight request, so without this flag the worker would go on to issue an untracked,
        // un-abortable call and leak its thread. The handler sets this before cancelling; the worker
        // re-checks it after publishing workerRef and bails if already set.
        final AtomicBoolean cancelled = new AtomicBoolean();
        final Future<T> future = operationExecutor.submit(() -> {
            workerRef.set(Thread.currentThread());
            if (cancelled.get()) {
                throw new StorageBackendException("Cancelled before execution: " + description);
            }
            try {
                return action.call();
            } finally {
                requestTracker.clear(Thread.currentThread());
            }
        });
        try {
            return future.get(operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            cancelled.set(true);
            cancelWorker(future, workerRef.get());
            throw new StorageBackendException(
                "Timed out after " + operationTimeout + " attempting to " + description, e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof StorageBackendException) {
                throw (StorageBackendException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new StorageBackendException("Failed to " + description, cause);
        } catch (final InterruptedException e) {
            // Same leak as the timeout path: a worker blocked in socketWrite0 won't be unblocked by
            // cancel(false), so we must abort its in-flight request. Interruption of the calling
            // thread is routine during RLM task cancellation (partition reassignment, broker
            // shutdown), so without this the worker + its socket would leak on every such event.
            cancelled.set(true);
            cancelWorker(future, workerRef.get());
            Thread.currentThread().interrupt();
            throw new StorageBackendException("Interrupted while attempting to " + description, e);
        }
    }

    /**
     * Abort a bounded call's worker and cancel its future. Force-closes the worker's in-flight
     * Apache request (via {@link AbortableRequestTracker#cancelAndAbort}) so a thread parked in
     * {@code socketWrite0} unblocks, then cancels the future.
     *
     * <p>Always {@code cancel(false)}, never {@code cancel(true)}: interrupting a worker parked in a
     * {@code java.nio.channels} writable channel makes the JVM's interrupt handler flush the
     * channel's output buffer onto the SAME blocked socket, parking the canceller alongside the
     * worker. The abort — not the interrupt — is what unblocks the worker.
     */
    private void cancelWorker(final Future<?> future, final Thread worker) {
        requestTracker.cancelAndAbort(worker);
        future.cancel(false);
    }

    private static <T> T runDirectly(final Callable<T> action, final String description)
        throws StorageBackendException {
        try {
            return action.call();
        } catch (final StorageBackendException e) {
            throw e;
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new StorageBackendException("Failed to " + description, e);
        }
    }

    private Blob getBlob(final ObjectKey key) throws KeyNotFoundException {
        // Unfortunately, it seems Google will do two a separate (HEAD-like) call to get blob metadata first
        // and then the actual download:
        // > Cloud Storage client libraries might use two or more operations to perform a task.
        // Source: https://cloud.google.com/storage/pricing
        // Since the blobs are immutable in tiered storage, we can consider caching them locally
        // to avoid the extra round trip.
        final Blob blob = storage.get(this.bucketName, key.value());
        if (blob == null) {
            throw new KeyNotFoundException(this, key);
        }
        return blob;
    }

    /**
     * Updates the storage client with new credentials.
     * This method is called when credentials are reloaded.
     *
     * @param credentials the new credentials to use
     */
    protected void updateStorageClient(final com.google.auth.Credentials credentials) {
        synchronized (this) {
            this.storage = storageOptionsBuilder
                .setCredentials(credentials)
                .build()
                .getService();
        }
    }

    @Override
    public void close() throws IOException {
        // Attempt to release every resource even if an earlier one throws, so a single failing
        // close() can't strand the others; rethrow the first failure with the rest suppressed.
        //
        // Shut the Apache transport down BEFORE the executor: closing the connection manager
        // force-closes in-flight sockets, so a blocked worker unwinds via SocketException.
        // Interrupting first (operationExecutor.shutdownNow() calls Thread.interrupt() on every
        // worker) risks parking this thread in the JDK's NIO interrupt handler for a worker stuck
        // in an interruptible channel — the same hazard runBounded avoids with cancel(false) — and,
        // worse, if it parked we'd never reach the transport shutdown that would unblock the worker.
        IOException failure = null;
        if (apacheTransport != null) {
            try {
                apacheTransport.shutdown();
            } catch (final IOException e) {
                failure = addSuppressed(failure, e);
            }
        }
        // shutdownNow() does not throw; with sockets already closed above, the interrupt is harmless.
        if (operationExecutor != null) {
            operationExecutor.shutdownNow();
        }
        if (credentialsProvider != null) {
            try {
                credentialsProvider.close();
            } catch (final IOException e) {
                failure = addSuppressed(failure, e);
            }
        }
        if (metricCollector != null) {
            try {
                metricCollector.close();
            } catch (final IOException e) {
                failure = addSuppressed(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Accumulate a close() failure: returns {@code next} when it is the first failure, otherwise
     * attaches {@code next} to {@code first} as a suppressed exception and returns {@code first}.
     * Package-private for testing.
     */
    static IOException addSuppressed(final IOException first, final IOException next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    @Override
    public String toString() {
        return "GCSStorage{"
            + "bucketName='" + bucketName + '\''
            + '}';
    }
}
