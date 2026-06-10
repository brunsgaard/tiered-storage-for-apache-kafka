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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lifecycle tests for {@link GcsStorage}: idempotent {@link GcsStorage#configure} (#5) and the
 * bounded-executor rejection surfacing as {@link StorageBackendException} (#4). Uses
 * {@code gcs.credentials.default=false} and a dummy endpoint so {@code configure()} builds a client
 * without touching the network (no call is actually made).
 */
class GcsStorageConfigureTest {

    private GcsStorage storage;

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
    }

    private static Map<String, Object> baseConfig() {
        return Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.endpoint.url", "http://localhost:65535",
            "gcs.credentials.default", "false",
            "gcs.operation.timeout", "3000"   // urlconnection transport -> warns, but exercises the executor
        );
    }

    @Test
    void reconfigureShutsDownThePriorExecutorAndDoesNotThrow() {
        storage = new GcsStorage();
        storage.configure(baseConfig());
        final ExecutorService first = storage.operationExecutor;
        assertThat(first).isNotNull();

        assertThatCode(() -> storage.configure(baseConfig())).doesNotThrowAnyException();

        assertThat(first.isShutdown())
            .as("the executor from the first configure() should be shut down by the second")
            .isTrue();
        assertThat(storage.operationExecutor)
            .as("a fresh executor should be in place after re-configure")
            .isNotNull()
            .isNotSameAs(first);
    }

    @Test
    void operationRejectedWhenExecutorSaturatedSurfacesAsStorageBackendException() throws Exception {
        storage = new GcsStorage();
        storage.configure(baseConfig());

        // Replace the pool with a max=1 pool and saturate it, so the next submit is rejected before
        // any network interaction. delete() goes through runBounded, which must translate the
        // RejectedExecutionException into a StorageBackendException rather than propagating it raw.
        final ThreadPoolExecutor saturated = GcsStorage.newOperationExecutor(1);
        storage.operationExecutor.shutdownNow();
        storage.operationExecutor = saturated;

        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch started = new CountDownLatch(1);
        try {
            saturated.submit(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> storage.delete(new TestObjectKey("any-key")))
                .isInstanceOf(StorageBackendException.class)
                .hasMessageContaining("Too many concurrent GCS operations");
        } finally {
            release.countDown();
            saturated.shutdownNow();
        }
    }
}
