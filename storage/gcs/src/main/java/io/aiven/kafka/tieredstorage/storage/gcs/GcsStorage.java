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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;
import io.aiven.kafka.tieredstorage.storage.proxy.Socks5ProxyAuthenticator;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GcsStorage implements StorageBackend {
    private volatile Storage storage;
    private String bucketName;
    private MetricCollector metricCollector;
    private Integer resumableUploadChunkSize;
    private ReloadableCredentialsProvider credentialsProvider;
    private StorageOptions.Builder storageOptionsBuilder;
    private Duration operationTimeout;
    private ExecutorService operationExecutor;

    @Override
    public void configure(final Map<String, ?> configs) {
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
        }

        metricCollector = new MetricCollector();

        // Create reloadable credentials provider
        this.credentialsProvider = config.reloadableCredentials();

        // Store the builder template for recreating storage clients. The write timeout and
        // retry settings are set on the template (not on the per-call client) so they are
        // preserved when the client is rebuilt on a credentials reload.
        this.storageOptionsBuilder = StorageOptions.newBuilder()
            .setTransportOptions(metricCollector.httpTransportOptions(
                httpTransportOptionsBuilder, config.httpWriteTimeout()));
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
            operationExecutor = Executors.newCachedThreadPool(r -> {
                final Thread t = new Thread(r, "gcs-operation");
                t.setDaemon(true);
                return t;
            });
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

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        return runBounded(() -> doUpload(inputStream, key), "upload " + key);
    }

    private long doUpload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
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
     * <p>On {@link TimeoutException}, throws {@link StorageBackendException} immediately. The
     * cancelled task continues running on its worker thread (because {@code socketWrite0} is a
     * native blocking call that does not honor {@link Thread#interrupt()}), but the caller is
     * unblocked. The leaked thread completes naturally when the kernel TCP retransmit window
     * expires.
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
        final Future<T> future = operationExecutor.submit(action);
        try {
            return future.get(operationTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            future.cancel(true);
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
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new StorageBackendException("Interrupted while attempting to " + description, e);
        }
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
        if (operationExecutor != null) {
            operationExecutor.shutdownNow();
        }
        if (credentialsProvider != null) {
            credentialsProvider.close();
        }
        if (metricCollector != null) {
            metricCollector.close();
        }
    }

    @Override
    public String toString() {
        return "GCSStorage{"
            + "bucketName='" + bucketName + '\''
            + '}';
    }
}
