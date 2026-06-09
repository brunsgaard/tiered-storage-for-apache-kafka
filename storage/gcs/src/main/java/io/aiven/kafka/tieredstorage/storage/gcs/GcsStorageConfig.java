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
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.tieredstorage.config.validators.NonEmptyPassword;
import io.aiven.kafka.tieredstorage.config.validators.Null;
import io.aiven.kafka.tieredstorage.config.validators.ValidUrl;
import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;

public class GcsStorageConfig extends AbstractConfig {
    static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";
    private static final String GCS_BUCKET_NAME_DOC = "GCS bucket to store log segments";

    static final String GCS_ENDPOINT_URL_CONFIG = "gcs.endpoint.url";
    private static final String GCS_ENDPOINT_URL_DOC = "Custom GCS endpoint URL. "
        + "To be used with custom GCS-compatible backends.";

    private static final String GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_CONFIG = "gcs.resumable.upload.chunk.size";
    private static final String GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DOC = "The chunk size for resumable upload. "
        + "Must be a multiple of 256 KiB (256 x 1024 bytes). "
        + "Larger chunk sizes typically make uploads faster, but requires bigger memory buffers. "
        + "The recommended minimum for GCS is 8 MiB. The SDK default is 15 MiB, "
        + "`see <https://cloud.google.com/storage/docs/resumable-uploads#java>`_. "
        + "The smaller the chunk size, the more calls to GCS are needed to upload a file; increasing costs. "
        + "The higher the chunk size, the more memory is needed to buffer the chunk.";
    static final int GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DEFAULT = 25 * 1024 * 1024; // 25MiB

    static final String GCS_HTTP_WRITE_TIMEOUT_CONFIG = "gcs.http.write.timeout";
    private static final String GCS_HTTP_WRITE_TIMEOUT_DOC =
        "Timeout in milliseconds for writing the request body to GCS, applied to every "
            + "HTTP request issued by the underlying transport. Bounds chunk PUT writes during "
            + "resumable upload. Without this setting, a half-open TCP connection can block the "
            + "upload thread until the kernel TCP retransmit window expires (15+ minutes on "
            + "default Linux), because java.net.HttpURLConnection has no socket-level write "
            + "timeout. When set, google-http-client bounds each write and throws IOException on "
            + "expiry, allowing the SDK to retry or surface the error. When unset, no write "
            + "timeout is applied.";

    static final String GCS_API_RETRY_TOTAL_TIMEOUT_CONFIG = "gcs.api.retry.total.timeout";
    private static final String GCS_API_RETRY_TOTAL_TIMEOUT_DOC =
        "Total timeout in milliseconds for a single GCS API call across retries, overriding "
            + "the SDK default. Bounds the cumulative time spent inside StorageOptions retry "
            + "loops (e.g. resumable upload chunk PUT retries). Combine with "
            + GCS_HTTP_WRITE_TIMEOUT_CONFIG + " to prevent indefinite retry-then-block cycles "
            + "during sustained network breakage. Note this is best-effort: the SDK's "
            + "resumable-upload path does not reliably honor it, so gcs.operation.timeout "
            + "is the authoritative backstop. When unset, the SDK default applies.";

    static final String GCS_API_RETRY_MAX_ATTEMPTS_CONFIG = "gcs.api.retry.max.attempts";
    private static final String GCS_API_RETRY_MAX_ATTEMPTS_DOC =
        "Maximum number of attempts for a single GCS API call, overriding the SDK default. "
            + "0 means unlimited (subject to " + GCS_API_RETRY_TOTAL_TIMEOUT_CONFIG + "). "
            + "When unset, the SDK default applies.";

    static final String GCS_HTTP_READ_TIMEOUT_CONFIG = "gcs.http.read.timeout";
    private static final String GCS_HTTP_READ_TIMEOUT_DOC =
        "Timeout in milliseconds for reading data from GCS, applied as the socket read "
            + "(SO_TIMEOUT) on every HTTP request issued by the underlying transport. This is the "
            + "read-path counterpart to " + GCS_HTTP_WRITE_TIMEOUT_CONFIG + ", and an inactivity "
            + "bound, not a total-transfer bound: a healthy large download is not interrupted; an "
            + "individual read attempt that stalls (e.g. a half-open TCP connection where no bytes "
            + "arrive) throws SocketTimeoutException after the timeout. IMPORTANT: this bounds each "
            + "read ATTEMPT, not the whole fetch. The google-cloud-storage ReadChannel transparently "
            + "reopens and retries a failed media download, and that reopen loop is not bounded by "
            + "gcs.operation.timeout (which only covers the metadata get, not the lazily "
            + "read stream) nor by gcs.api.retry.* (verified against google-cloud-storage 2.61.0). So "
            + "on a PERSISTENT read stall this caps per-attempt latency and keeps the worker active "
            + "(not blocked) but does not make fetch() fail fast. Applies to both transports. When "
            + "unset, the SDK default applies.";

    static final String GCS_HTTP_CONNECT_TIMEOUT_CONFIG = "gcs.http.connect.timeout";
    private static final String GCS_HTTP_CONNECT_TIMEOUT_DOC =
        "Timeout in milliseconds for establishing the TCP connection to GCS, applied to every HTTP "
            + "request issued by the underlying transport. Bounds the time spent in connect() against "
            + "an unreachable or black-holed endpoint. Applies to both transports. When unset, the "
            + "SDK default applies.";

    static final String GCS_HTTP_TRANSPORT_CONFIG = "gcs.http.transport";
    static final String GCS_HTTP_TRANSPORT_URLCONNECTION = "urlconnection";
    static final String GCS_HTTP_TRANSPORT_APACHE = "apache";
    private static final String GCS_HTTP_TRANSPORT_DOC =
        "HTTP transport implementation for the GCS client. "
            + "'" + GCS_HTTP_TRANSPORT_URLCONNECTION + "' (default) uses java.net.HttpURLConnection "
            + "via google-http-client's NetHttpTransport. "
            + "'" + GCS_HTTP_TRANSPORT_APACHE + "' uses Apache HttpClient via ApacheHttpTransport, "
            + "which exposes a per-request abort() that the plugin uses on the gcs.operation.timeout "
            + "to force-close the underlying socket. With urlconnection, an in-flight write that "
            + "has filled the kernel send buffer cannot be cancelled mid-flight (the worker thread "
            + "leaks until the kernel TCP retransmit window expires). With apache, the worker thread "
            + "is unblocked promptly when the call timeout fires.";

    static final String GCS_OPERATION_TIMEOUT_CONFIG = "gcs.operation.timeout";
    private static final String GCS_OPERATION_TIMEOUT_DOC =
        "Hard upper bound in milliseconds on a single plugin-level call to GCS "
            + "(upload, fetch, delete). When set, the call runs on a separate executor "
            + "and is bounded with Future.get(timeout); on expiry the plugin throws "
            + "StorageBackendException regardless of what the SDK is doing internally. "
            + "This is the outermost wall around the SDK's retry layers, which empirically "
            + "do not always honor " + GCS_API_RETRY_TOTAL_TIMEOUT_CONFIG + " for resumable "
            + "uploads, giving callers a deterministic time-to-failure when GCS or the network "
            + "path is misbehaving. On expiry the in-flight request is aborted via the required "
            + GCS_HTTP_TRANSPORT_APACHE + " transport (which force-closes the socket), so the worker "
            + "thread unwinds promptly instead of leaking. When unset, calls run synchronously with "
            + "no plugin-level bound.";

    static final String GCP_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    static final String GCP_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    static final String GCP_CREDENTIALS_DEFAULT_CONFIG = "gcs.credentials.default";

    private static final String GCP_CREDENTIALS_JSON_DOC = "GCP credentials as a JSON string. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_PATH_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_DEFAULT_CONFIG + "\"";
    private static final String GCP_CREDENTIALS_PATH_DOC = "The path to a GCP credentials file. "
        + "This can be standard GCP credentials format, or JSON with a single `access_token` field. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_JSON_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_DEFAULT_CONFIG + "\"";
    private static final String GCP_CREDENTIALS_DEFAULT_DOC = "Use the default GCP credentials. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_JSON_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_PATH_CONFIG + "\"";

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                GCS_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                GCS_BUCKET_NAME_DOC)
            .define(
                GCS_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ValidUrl(),
                ConfigDef.Importance.LOW,
                GCS_ENDPOINT_URL_DOC)
            .define(
                GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_CONFIG,
                ConfigDef.Type.INT,
                GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DEFAULT,
                new ResumableUploadChunkSizeValidator(),
                ConfigDef.Importance.MEDIUM,
                GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DOC)
            .define(
                GCS_HTTP_WRITE_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1L, (long) Integer.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                GCS_HTTP_WRITE_TIMEOUT_DOC)
            .define(
                GCS_HTTP_READ_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1L, (long) Integer.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                GCS_HTTP_READ_TIMEOUT_DOC)
            .define(
                GCS_HTTP_CONNECT_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1L, (long) Integer.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                GCS_HTTP_CONNECT_TIMEOUT_DOC)
            .define(
                GCS_API_RETRY_TOTAL_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1L, Long.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                GCS_API_RETRY_TOTAL_TIMEOUT_DOC)
            .define(
                GCS_API_RETRY_MAX_ATTEMPTS_CONFIG,
                ConfigDef.Type.INT,
                null,
                Null.or(ConfigDef.Range.between(0, Integer.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                GCS_API_RETRY_MAX_ATTEMPTS_DOC)
            .define(
                GCS_OPERATION_TIMEOUT_CONFIG,
                ConfigDef.Type.LONG,
                null,
                Null.or(ConfigDef.Range.between(1L, Long.MAX_VALUE)),
                ConfigDef.Importance.LOW,
                GCS_OPERATION_TIMEOUT_DOC)
            .define(
                GCS_HTTP_TRANSPORT_CONFIG,
                ConfigDef.Type.STRING,
                GCS_HTTP_TRANSPORT_URLCONNECTION,
                ConfigDef.ValidString.in(GCS_HTTP_TRANSPORT_URLCONNECTION, GCS_HTTP_TRANSPORT_APACHE),
                ConfigDef.Importance.LOW,
                GCS_HTTP_TRANSPORT_DOC)
            .define(
                GCP_CREDENTIALS_JSON_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_JSON_DOC)
            .define(
                GCP_CREDENTIALS_PATH_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_PATH_DOC)
            .define(
                GCP_CREDENTIALS_DEFAULT_CONFIG,
                ConfigDef.Type.BOOLEAN,
                null,
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_DEFAULT_DOC);
    }

    private ProxyConfig proxyConfig = null;

    public GcsStorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
        validate();

        final Map<String, ?> proxyProps = this.originalsWithPrefix(ProxyConfig.PROXY_PREFIX, true);
        if (!proxyProps.isEmpty()) {
            this.proxyConfig = new ProxyConfig(proxyProps);
        }
    }

    ProxyConfig proxyConfig() {
        return proxyConfig;
    }

    private void validate() {
        final String credentialsJson = getPassword(GCP_CREDENTIALS_JSON_CONFIG) == null
            ? null
            : getPassword(GCP_CREDENTIALS_JSON_CONFIG).value();

        try {
            CredentialsBuilder.validate(
                getBoolean(GCP_CREDENTIALS_DEFAULT_CONFIG),
                credentialsJson,
                getString(GCP_CREDENTIALS_PATH_CONFIG)
            );
        } catch (final IllegalArgumentException e) {
            final String message = e.getMessage()
                .replace("credentialsPath", GCP_CREDENTIALS_PATH_CONFIG)
                .replace("credentialsJson", GCP_CREDENTIALS_JSON_CONFIG)
                .replace("defaultCredentials", GCP_CREDENTIALS_DEFAULT_CONFIG);
            throw new ConfigException(message);
        }

        // The Apache transport exists only to make a call bounded by gcs.operation.timeout abortable;
        // without that timeout its request-tracking interceptor never serves a purpose and just
        // retains stale per-thread state. Reject the inert combination rather than silently degrading.
        // (Note: no ordering constraint between call.timeout and write.timeout — the intended use is
        // often a SHORT call.timeout as a fast abort wall with a longer write.timeout backstop.)
        if (GCS_HTTP_TRANSPORT_APACHE.equals(getString(GCS_HTTP_TRANSPORT_CONFIG))
            && getLong(GCS_OPERATION_TIMEOUT_CONFIG) == null) {
            throw new ConfigException(GCS_HTTP_TRANSPORT_CONFIG + "=" + GCS_HTTP_TRANSPORT_APACHE
                + " requires " + GCS_OPERATION_TIMEOUT_CONFIG + " to be set; the Apache transport is "
                + "only useful as the lever that makes a " + GCS_OPERATION_TIMEOUT_CONFIG + "-bounded call "
                + "abortable.");
        }

        // ...and the inverse: operation.timeout is only meaningful on a transport whose in-flight
        // request can be aborted. On any other transport an expired timeout fails the call but leaves
        // the worker blocked until the OS tears the socket down — i.e. it leaks a thread and the
        // bound is only half-real. Require apache rather than silently under-deliver. (The two are
        // thus mutually required: set both, or neither.)
        if (getLong(GCS_OPERATION_TIMEOUT_CONFIG) != null
            && !GCS_HTTP_TRANSPORT_APACHE.equals(getString(GCS_HTTP_TRANSPORT_CONFIG))) {
            throw new ConfigException(GCS_OPERATION_TIMEOUT_CONFIG + " requires "
                + GCS_HTTP_TRANSPORT_CONFIG + "=" + GCS_HTTP_TRANSPORT_APACHE
                + "; only the Apache transport can abort an in-flight request on timeout, so on any "
                + "other transport the timeout would fail the call but leak its worker thread.");
        }
    }

    String bucketName() {
        return getString(GCS_BUCKET_NAME_CONFIG);
    }

    String endpointUrl() {
        return getString(GCS_ENDPOINT_URL_CONFIG);
    }

    Integer resumableUploadChunkSize() {
        return getInt(GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_CONFIG);
    }

    Duration httpWriteTimeout() {
        return getDurationMillis(GCS_HTTP_WRITE_TIMEOUT_CONFIG);
    }

    Duration httpReadTimeout() {
        return getDurationMillis(GCS_HTTP_READ_TIMEOUT_CONFIG);
    }

    Duration httpConnectTimeout() {
        return getDurationMillis(GCS_HTTP_CONNECT_TIMEOUT_CONFIG);
    }

    Duration apiRetryTotalTimeout() {
        return getDurationMillis(GCS_API_RETRY_TOTAL_TIMEOUT_CONFIG);
    }

    Integer apiRetryMaxAttempts() {
        return getInt(GCS_API_RETRY_MAX_ATTEMPTS_CONFIG);
    }

    Duration operationTimeout() {
        return getDurationMillis(GCS_OPERATION_TIMEOUT_CONFIG);
    }

    String httpTransport() {
        return getString(GCS_HTTP_TRANSPORT_CONFIG);
    }

    private Duration getDurationMillis(final String key) {
        final Long value = getLong(key);
        return value == null ? null : Duration.ofMillis(value);
    }

    /**
     * Creates a reloadable credentials provider that automatically reloads credentials
     * when the credentials file is modified during runtime (if using file-based credentials).
     *
     * @return a ReloadableCredentialsProvider instance
     * @throws ConfigException if credentials cannot be created
     */
    ReloadableCredentialsProvider reloadableCredentials() {
        final Boolean defaultCredentials = getBoolean(GCP_CREDENTIALS_DEFAULT_CONFIG);
        final Password credentialsJsonPwd = getPassword(GCP_CREDENTIALS_JSON_CONFIG);
        final String credentialsJson = credentialsJsonPwd == null ? null : credentialsJsonPwd.value();
        final String credentialsPath = getString(GCP_CREDENTIALS_PATH_CONFIG);

        try {
            return new ReloadableCredentialsProvider(defaultCredentials, credentialsJson, credentialsPath);
        } catch (final IOException e) {
            throw new ConfigException("Failed to create GCS credentials: " + e.getMessage());
        }
    }

    private static class ResumableUploadChunkSizeValidator implements ConfigDef.Validator {
        private static final long MULTIPLIER = 256L * 1024;

        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                return;
            }
            final int intValue = (int) value;
            if (intValue < MULTIPLIER) {
                throw new ConfigException(name, value, "Value must be at least 256 KiB (" + MULTIPLIER + " B)");
            }
            if (intValue % MULTIPLIER != 0) {
                throw new ConfigException(name, value, "Value must be a multiple of 256 KiB (" + MULTIPLIER + " B)");
            }
        }

        @Override
        public String toString() {
            return "[256 KiB...] values multiple of " + MULTIPLIER + " bytes";
        }
    }
}
