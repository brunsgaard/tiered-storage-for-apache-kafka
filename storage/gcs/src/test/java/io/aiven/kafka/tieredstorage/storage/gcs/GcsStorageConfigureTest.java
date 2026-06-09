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
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Lifecycle test for idempotent {@link GcsStorage#configure}. Uses {@code gcs.credentials.default=false}
 * and a dummy endpoint so {@code configure()} builds a client without touching the network, and the
 * apache transport (which {@code gcs.operation.timeout} requires) so the operation executor is created.
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
            "gcs.http.transport", "apache",       // required by gcs.operation.timeout
            "gcs.operation.timeout", "3000"        // present so the operation executor is created
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
}
