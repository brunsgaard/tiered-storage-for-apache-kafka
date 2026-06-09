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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that {@link GcsStorage#buildRetrySettings} composes the user's overrides
 * on top of the SDK defaults correctly.
 *
 * <p>Why this test exists: {@code buildRetrySettings} is the only machinery behind
 * {@code gcs.api.retry.{total.timeout,max.attempts}}. The integration tests exercise these
 * configs end-to-end but assert wall-clock elapsed; they would not catch a bug where
 * {@code buildRetrySettings} returns a {@link RetrySettings} with the wrong fields, because
 * the resumable-upload resume layer above the SDK's {@code Retrying} does not honor these
 * settings anyway. This test is the regression guard for the helper itself.
 */
class GcsStorageRetrySettingsTest {

    private static final RetrySettings DEFAULTS = StorageOptions.getDefaultRetrySettings();

    @Test
    void bothNullReturnsNull() {
        // Signal to the caller "no override; leave the SDK default untouched".
        // GcsStorage.configure relies on this null sentinel.
        assertThat(GcsStorage.buildRetrySettings(null, null)).isNull();
    }

    @Test
    void totalTimeoutOnlyOverridesTotalTimeout() {
        final Duration override = Duration.ofMillis(7_500);
        final RetrySettings rs = GcsStorage.buildRetrySettings(override, null);

        assertThat(rs).isNotNull();
        assertThat(rs.getTotalTimeoutDuration()).isEqualTo(override);
        // maxAttempts and the rest carry over from the SDK default unchanged.
        assertThat(rs.getMaxAttempts()).isEqualTo(DEFAULTS.getMaxAttempts());
        assertThat(rs.getInitialRetryDelayDuration())
            .isEqualTo(DEFAULTS.getInitialRetryDelayDuration());
        assertThat(rs.getMaxRetryDelayDuration())
            .isEqualTo(DEFAULTS.getMaxRetryDelayDuration());
        assertThat(rs.getRetryDelayMultiplier())
            .isEqualTo(DEFAULTS.getRetryDelayMultiplier());
    }

    @Test
    void maxAttemptsOnlyOverridesMaxAttempts() {
        final RetrySettings rs = GcsStorage.buildRetrySettings(null, 1);

        assertThat(rs).isNotNull();
        assertThat(rs.getMaxAttempts()).isEqualTo(1);
        // totalTimeout and the rest carry over from the SDK default unchanged.
        assertThat(rs.getTotalTimeoutDuration())
            .isEqualTo(DEFAULTS.getTotalTimeoutDuration());
        assertThat(rs.getInitialRetryDelayDuration())
            .isEqualTo(DEFAULTS.getInitialRetryDelayDuration());
    }

    @Test
    void bothSetOverridesBoth() {
        final Duration totalTimeout = Duration.ofMillis(30_000);
        final Integer maxAttempts = 3;
        final RetrySettings rs = GcsStorage.buildRetrySettings(totalTimeout, maxAttempts);

        assertThat(rs).isNotNull();
        assertThat(rs.getTotalTimeoutDuration()).isEqualTo(totalTimeout);
        assertThat(rs.getMaxAttempts()).isEqualTo(maxAttempts);
        // Everything else still inherits from the default — the operator overrode
        // exactly two fields, the rest of the retry shape is the SDK's.
        assertThat(rs.getInitialRetryDelayDuration())
            .isEqualTo(DEFAULTS.getInitialRetryDelayDuration());
        assertThat(rs.getMaxRetryDelayDuration())
            .isEqualTo(DEFAULTS.getMaxRetryDelayDuration());
        assertThat(rs.getRetryDelayMultiplier())
            .isEqualTo(DEFAULTS.getRetryDelayMultiplier());
    }

    @Test
    void maxAttemptsZeroIsAllowed() {
        // gax semantics: maxAttempts=0 means "unlimited". The plugin's config validator
        // allows this. buildRetrySettings passes it through; the resulting RetrySettings's
        // getMaxAttempts() returns 0, which the SDK's Retrying layer treats as unlimited.
        final RetrySettings rs = GcsStorage.buildRetrySettings(null, 0);
        assertThat(rs).isNotNull();
        assertThat(rs.getMaxAttempts()).isEqualTo(0);
    }

    @Test
    void sdkDefaultsAreIntactAtFloor() {
        // Sanity check that StorageOptions.getDefaultRetrySettings() — the base we override
        // on top of — has the values we expect from the SDK. If a future SDK upgrade changes
        // these, this test surfaces it so the override behavior can be re-reviewed.
        assertThat(DEFAULTS.getMaxAttempts()).isEqualTo(6);
        assertThat(DEFAULTS.getInitialRetryDelayDuration()).isEqualTo(Duration.ofSeconds(1));
        assertThat(DEFAULTS.getMaxRetryDelayDuration()).isEqualTo(Duration.ofSeconds(32));
        assertThat(DEFAULTS.getRetryDelayMultiplier()).isEqualTo(2.0);
        assertThat(DEFAULTS.getTotalTimeoutDuration()).isEqualTo(Duration.ofSeconds(50));
    }
}
