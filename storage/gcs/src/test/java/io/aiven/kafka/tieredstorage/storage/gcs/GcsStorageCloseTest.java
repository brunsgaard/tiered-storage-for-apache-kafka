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

import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link GcsStorage#addSuppressed}, the helper {@code close()} uses to aggregate
 * cleanup failures so that one failing {@code close()} cannot strand the others.
 */
class GcsStorageCloseTest {

    @Test
    void firstFailureIsReturnedAsIs() {
        final IOException first = new IOException("first");
        assertThat(GcsStorage.addSuppressed(null, first)).isSameAs(first);
        assertThat(first.getSuppressed()).isEmpty();
    }

    @Test
    void subsequentFailuresAreAttachedAsSuppressed() {
        final IOException first = new IOException("first");
        final IOException second = new IOException("second");
        final IOException third = new IOException("third");

        IOException acc = GcsStorage.addSuppressed(null, first);
        acc = GcsStorage.addSuppressed(acc, second);
        acc = GcsStorage.addSuppressed(acc, third);

        assertThat(acc).isSameAs(first);
        assertThat(acc.getSuppressed()).containsExactly(second, third);
    }
}
