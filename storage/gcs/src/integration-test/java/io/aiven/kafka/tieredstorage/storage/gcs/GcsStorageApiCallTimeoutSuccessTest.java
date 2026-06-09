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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that {@code gcs.operation.timeout}'s executor-wrapped path in
 * {@link GcsStorage#upload}/{@link GcsStorage#fetch}/{@link GcsStorage#delete} does NOT regress the
 * success path. Parameterized across both transports ({@code urlconnection} and {@code apache})
 * since each wires through {@code runBounded} differently.
 *
 * <p>Sister to {@link GcsStorageWriteTimeoutTest}: that one validates failure-mode cancellation;
 * this one validates that under healthy conditions, results and exceptions still propagate
 * correctly through the {@link java.util.concurrent.Future} +
 * {@link java.util.concurrent.ExecutionException} unwrapping.
 */
@Testcontainers
class GcsStorageApiCallTimeoutSuccessTest {
    @Container
    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer();

    static Storage storage;
    private String bucketName;

    @BeforeAll
    static void setUpClass() {
        storage = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(GCS_SERVER.url())
            .setProjectId("test-project")
            .build()
            .getService();
    }

    @BeforeEach
    void setUp() {
        // Build a fresh, GCS-legal bucket name per test invocation (parameterized test display
        // names include brackets and parentheses, which fake-gcs-server rejects).
        bucketName = "apicall-timeout-success-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        storage.create(BucketInfo.newBuilder(bucketName).build());
    }

    @ParameterizedTest(name = "transport={0}")
    @ValueSource(strings = {"urlconnection", "apache"})
    void uploadFetchDeleteRoundtripSucceedsWithApiCallTimeoutSet(final String transport) throws Exception {
        // 60s call timeout: comfortably above any healthy fake-gcs-server roundtrip, so it never
        // fires; this is a success-path test. The point is to exercise the executor-wrapped code
        // path in GcsStorage.runBounded.
        final Map<String, Object> configs = new HashMap<>();
        configs.put("gcs.bucket.name", bucketName);
        configs.put("gcs.endpoint.url", GCS_SERVER.url());
        configs.put("gcs.credentials.default", "false");
        configs.put("gcs.http.transport", transport);
        configs.put("gcs.operation.timeout", "60000");

        final GcsStorage gcsStorage = new GcsStorage();
        gcsStorage.configure(configs);

        final byte[] payload = "hello via runBounded".getBytes(StandardCharsets.UTF_8);
        final TestObjectKey key = new TestObjectKey("roundtrip/" + transport + ".txt");

        // Upload — exercises runBounded -> doUpload -> Storage.createFrom
        final long uploaded = gcsStorage.upload(new ByteArrayInputStream(payload), key);
        assertThat(uploaded).isEqualTo(payload.length);

        // Full fetch — exercises runBounded -> doFetch
        try (final InputStream in = gcsStorage.fetch(key)) {
            assertThat(in.readAllBytes()).isEqualTo(payload);
        }

        // Range fetch — exercises runBounded -> doFetchRange
        try (final InputStream in = gcsStorage.fetch(key, BytesRange.of(6, 9))) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("via ");
        }

        // Delete — exercises runBounded -> doDelete (Void return type via the lambda)
        gcsStorage.delete(key);

        // Re-fetch should now fail with KeyNotFoundException, propagated through
        // ExecutionException unwrapping in runBounded.
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> gcsStorage.fetch(key))
            .isInstanceOf(io.aiven.kafka.tieredstorage.storage.KeyNotFoundException.class);
    }
}
