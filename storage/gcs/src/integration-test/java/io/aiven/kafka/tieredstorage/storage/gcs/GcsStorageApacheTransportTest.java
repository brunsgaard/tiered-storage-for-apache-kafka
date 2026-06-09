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

import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.TestUtils;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the full {@link BaseStorageTest} parameterized suite against the GCS backend configured with
 * {@code gcs.http.transport=apache}, to exercise the broader success-path edge cases (range reads,
 * idempotent delete on missing keys, 416-out-of-range, KeyNotFoundException semantics, etc.) on the
 * Apache transport code path — not just the failure-mode and round-trip tests in
 * {@link GcsStorageWriteTimeoutTest} and {@link GcsStorageApiCallTimeoutSuccessTest}.
 *
 * <p>Sister to {@link GcsStorageTest}, which runs the same suite against the default
 * (urlconnection) transport. If both pass, the Apache transport opt-in is at parity with the
 * default for all observable behaviors {@code BaseStorageTest} checks.
 */
@Testcontainers
class GcsStorageApacheTransportTest extends BaseStorageTest {
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
    void setUp(final TestInfo testInfo) {
        bucketName = TestUtils.testNameToBucketName(testInfo);
        storage.create(BucketInfo.newBuilder(bucketName).build());
    }

    @Override
    protected StorageBackend storage() {
        final GcsStorage gcsStorage = new GcsStorage();
        gcsStorage.configure(Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.endpoint.url", GCS_SERVER.url(),
            "gcs.credentials.default", "false",
            "gcs.http.transport", "apache",
            // apache transport requires a call timeout (it exists to make the bounded call
            // abortable); generous here so it never trips on the success-path operations under test.
            "gcs.operation.timeout", "60000"
        ));
        return gcsStorage;
    }
}
