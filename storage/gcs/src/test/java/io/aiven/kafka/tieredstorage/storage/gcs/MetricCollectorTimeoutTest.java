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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that {@link MetricCollector#httpTransportOptions} plumbs the write, read, and connect
 * timeouts through to the per-request {@link HttpRequest} setters.
 *
 * <p>These are the load-bearing assertions behind {@code gcs.http.write.timeout},
 * {@code gcs.http.read.timeout}, and {@code gcs.http.connect.timeout}: the values are applied to
 * every request the SDK issues — including the media-download GETs backing {@code fetch()}'s
 * lazily-read stream — which is what bounds the read path that {@code gcs.operation.timeout} does
 * not reach (its bound returns before the stream's bytes are pulled).
 */
class MetricCollectorTimeoutTest {

    // HttpTransportOptions defaults both connect and read timeouts to 20s. It does not set a write
    // timeout, so HttpRequest's own default (0 = unbounded) is left in place when writeTimeout is null.
    private static final int TRANSPORT_DEFAULT_TIMEOUT_MS = 20_000;

    @Test
    void writeTimeoutIsAppliedToHttpRequest() throws Exception {
        final HttpRequest request = buildInitializedRequest(Duration.ofSeconds(45), null, null);
        assertThat(request.getWriteTimeout()).isEqualTo(45_000);
    }

    @Test
    void writeTimeoutNullLeavesDefault() throws Exception {
        final HttpRequest request = buildInitializedRequest(null, null, null);
        assertThat(request.getWriteTimeout()).isEqualTo(0);
    }

    @Test
    void readTimeoutIsAppliedToHttpRequest() throws Exception {
        final HttpRequest request = buildInitializedRequest(null, Duration.ofSeconds(30), null);
        assertThat(request.getReadTimeout()).isEqualTo(30_000);
    }

    @Test
    void readTimeoutNullLeavesTransportDefault() throws Exception {
        final HttpRequest request = buildInitializedRequest(null, null, null);
        assertThat(request.getReadTimeout()).isEqualTo(TRANSPORT_DEFAULT_TIMEOUT_MS);
    }

    @Test
    void connectTimeoutIsAppliedToHttpRequest() throws Exception {
        final HttpRequest request = buildInitializedRequest(null, null, Duration.ofSeconds(10));
        assertThat(request.getConnectTimeout()).isEqualTo(10_000);
    }

    @Test
    void connectTimeoutNullLeavesTransportDefault() throws Exception {
        final HttpRequest request = buildInitializedRequest(null, null, null);
        assertThat(request.getConnectTimeout()).isEqualTo(TRANSPORT_DEFAULT_TIMEOUT_MS);
    }

    @Test
    void allTimeoutsAreAppliedIndependently() throws Exception {
        final HttpRequest request = buildInitializedRequest(
            Duration.ofSeconds(45), Duration.ofSeconds(30), Duration.ofSeconds(10));
        assertThat(request.getWriteTimeout()).isEqualTo(45_000);
        assertThat(request.getReadTimeout()).isEqualTo(30_000);
        assertThat(request.getConnectTimeout()).isEqualTo(10_000);
    }

    private static HttpRequest buildInitializedRequest(final Duration writeTimeout,
                                                       final Duration readTimeout,
                                                       final Duration connectTimeout) throws Exception {
        final HttpTransport transport = new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(final String method, final String url) {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() {
                        return new MockLowLevelHttpResponse();
                    }
                };
            }
        };

        final var transportOptions = new MetricCollector().httpTransportOptions(
            com.google.cloud.http.HttpTransportOptions.newBuilder()
                .setHttpTransportFactory(() -> transport),
            writeTimeout, readTimeout, connectTimeout);

        // Build a Storage so we can borrow its ServiceOptions to feed the initializer.
        final var storageOptions = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setProjectId("test-project")
            .setTransportOptions(transportOptions)
            .build();

        final var initializer = transportOptions.getHttpRequestInitializer(storageOptions);
        final HttpRequest request = transport.createRequestFactory()
            .buildGetRequest(new GenericUrl("https://example.invalid/"));
        initializer.initialize(request);
        return request;
    }
}
