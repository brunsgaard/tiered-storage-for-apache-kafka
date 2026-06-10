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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link GcsStorage#newOperationExecutor(int)} is a bounded, fail-fast pool: capped thread
 * count, no work queue, abort-on-saturation, and daemon worker threads. This is the safety bound
 * (#4) that prevents stalled bounded calls from spawning threads/sockets without limit.
 */
class GcsStorageOperationExecutorTest {

    @Test
    void executorIsBoundedAndFailFast() {
        final ThreadPoolExecutor executor = GcsStorage.newOperationExecutor(7);
        try {
            assertThat(executor.getCorePoolSize()).isZero();
            assertThat(executor.getMaximumPoolSize()).isEqualTo(7);
            assertThat(executor.getQueue()).isInstanceOf(SynchronousQueue.class);
            assertThat(executor.getRejectedExecutionHandler())
                .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void workerThreadsAreNamedDaemons() throws Exception {
        final ThreadPoolExecutor executor = GcsStorage.newOperationExecutor(1);
        try {
            final AtomicReference<Thread> worker = new AtomicReference<>();
            final CountDownLatch ran = new CountDownLatch(1);
            executor.submit(() -> {
                worker.set(Thread.currentThread());
                ran.countDown();
            });
            assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(worker.get().isDaemon()).isTrue();
            assertThat(worker.get().getName()).isEqualTo("gcs-operation");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void submitBeyondCapacityIsRejected() throws Exception {
        final ThreadPoolExecutor executor = GcsStorage.newOperationExecutor(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch started = new CountDownLatch(1);
        try {
            // Occupy the single thread so the pool is saturated.
            executor.submit(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            // SynchronousQueue has no taker and the max pool size is reached -> reject.
            assertThatThrownBy(() -> executor.submit(() -> { }))
                .isInstanceOf(RejectedExecutionException.class);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
