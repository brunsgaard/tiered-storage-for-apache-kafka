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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link GcsStorage#newOperationExecutor()} produces daemon worker threads with unique,
 * recognizable names. The pool is intentionally unbounded (its concurrency is bounded by the
 * broker's RemoteLogManager pools, which call into it), so there is nothing to assert about a cap.
 */
class GcsStorageOperationExecutorTest {

    @Test
    void workerThreadsAreNamedDaemons() throws Exception {
        final ExecutorService executor = GcsStorage.newOperationExecutor();
        try {
            final AtomicReference<Thread> worker = new AtomicReference<>();
            final CountDownLatch ran = new CountDownLatch(1);
            executor.submit(() -> {
                worker.set(Thread.currentThread());
                ran.countDown();
            });
            assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(worker.get().isDaemon()).isTrue();
            assertThat(worker.get().getName()).matches("gcs-operation-\\d+");
        } finally {
            executor.shutdownNow();
        }
    }
}
