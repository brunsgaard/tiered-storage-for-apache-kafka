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
 * Verify that {@link MetricCollector#httpTransportOptions} plumbs the writeTimeout
 * through to the per-request {@link HttpRequest#setWriteTimeout(int)} call.
 *
 * <p>This is the load-bearing assertion behind the {@code gcs.http.write.timeout} setting:
 * without it, {@code NetHttpRequest.writeContentToOutputStream} takes the unbounded write
 * path, and a half-open TCP connection blocks the upload thread until the kernel TCP
 * retransmit window expires.
 */
class MetricCollectorWriteTimeoutTest {

    @Test
    void writeTimeoutIsAppliedToHttpRequest() throws Exception {
        final HttpRequest request = buildInitializedRequest(Duration.ofSeconds(45));
        assertThat(request.getWriteTimeout()).isEqualTo(45_000);
    }

    @Test
    void writeTimeoutNullLeavesDefault() throws Exception {
        // HttpRequest's default writeTimeout is 0 (unbounded for NetHttpTransport).
        final HttpRequest request = buildInitializedRequest(null);
        assertThat(request.getWriteTimeout()).isEqualTo(0);
    }

    private static HttpRequest buildInitializedRequest(final Duration writeTimeout) throws Exception {
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
            writeTimeout, null, null);

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
