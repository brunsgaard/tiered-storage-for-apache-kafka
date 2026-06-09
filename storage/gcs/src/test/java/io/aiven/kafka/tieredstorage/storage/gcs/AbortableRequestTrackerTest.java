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

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.params.HttpParams;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic regression guard for the interceptor / canceller race in
 * {@link AbortableRequestTracker#registerOrAbort}.
 *
 * <p>The race in the naive (pre-fix) ordering was:
 *
 * <pre>
 *   Worker thread (interceptor):       Canceller thread (timeout fired):
 *   [A] if cancelledWorkers.contains   (not yet)
 *                                       cancelledWorkers.add(worker)
 *                                       activeRequests.remove(worker) -> null
 *                                       skip abort (req is null)
 *   [B] activeRequests.put(...)
 *   request proceeds, parks in socketWrite0, is never aborted -> worker leaks
 * </pre>
 *
 * <p>The fix adds a re-check at [C] after the put. If the canceller raced in, [C] sees the flag and
 * aborts. The race interleave is exercised deterministically by overriding the
 * {@link AbortableRequestTracker#afterRegister} seam, which fires exactly between the [B] put and
 * the [C] re-check, to simulate the canceller landing in that window.
 *
 * <p>Uses a hand-rolled {@link RecordingRequest} stub instead of Mockito so the test doesn't depend
 * on the bytebuddy agent's self-attach (which is flaky in minimal Docker images that lack
 * process-attach capabilities).
 */
class AbortableRequestTrackerTest {

    @Test
    void normalPathRegistersRequestWithoutAborting() {
        final AbortableRequestTracker tracker = new AbortableRequestTracker();
        final RecordingRequest request = new RecordingRequest();
        final Thread current = Thread.currentThread();

        tracker.registerOrAbort(request);

        assertThat(request.abortCount.get()).isZero();
        assertThat(tracker.activeRequests).containsEntry(current, request);
        assertThat(tracker.cancelledWorkers).doesNotContain(current);
    }

    @Test
    void fastPathAbortsWhenCancelledBeforeCheck() {
        final AbortableRequestTracker tracker = new AbortableRequestTracker();
        final RecordingRequest request = new RecordingRequest();
        final Thread current = Thread.currentThread();

        // Simulate the canceller having fired BEFORE the worker reached the interceptor. The fast
        // path at [A] should abort and never register.
        tracker.cancelledWorkers.add(current);

        tracker.registerOrAbort(request);

        assertThat(request.abortCount.get()).isEqualTo(1);
        assertThat(tracker.activeRequests).doesNotContainKey(current);
    }

    @Test
    void recheckAfterPutCatchesCancellerLandingInRaceWindow() {
        final RecordingRequest request = new RecordingRequest();
        final Thread current = Thread.currentThread();
        // Inject the canceller's cancelledWorkers.add into the [B]->[C] gap. This deterministically
        // reproduces the race that occurs under timing pressure (the call timeout firing while a
        // worker is in the middle of the interceptor).
        final AbortableRequestTracker tracker = new AbortableRequestTracker() {
            @Override
            void afterRegister(final Thread worker) {
                cancelledWorkers.add(worker);
            }
        };

        tracker.registerOrAbort(request);

        // With the [C] re-check: abort is called once, request is removed. Without the re-check,
        // abort would NEVER be called and the request would stay in the map forever — the race
        // we're guarding against.
        assertThat(request.abortCount.get()).isEqualTo(1);
        assertThat(tracker.activeRequests).doesNotContainKey(current);
        assertThat(tracker.cancelledWorkers).contains(current);
    }

    @Test
    void recheckIsIdempotentWhenCancellerAlsoRemoves() {
        final RecordingRequest request = new RecordingRequest();
        final Thread current = Thread.currentThread();
        // Simulate the canceller's activeRequests.remove succeeding (it fired AFTER our put but
        // BEFORE our re-check). The re-check path must tolerate the request already being gone.
        final AbortableRequestTracker tracker = new AbortableRequestTracker() {
            @Override
            void afterRegister(final Thread worker) {
                cancelledWorkers.add(worker);
                activeRequests.remove(worker);
            }
        };

        tracker.registerOrAbort(request);

        assertThat(request.abortCount.get()).isEqualTo(1);
        assertThat(tracker.activeRequests).doesNotContainKey(current);
    }

    /**
     * Minimal {@link HttpUriRequest} stub that records {@link #abort()} calls. All other interface
     * methods throw {@link UnsupportedOperationException} — registerOrAbort only ever touches
     * abort(), so failing fast on any other call surfaces accidental coupling.
     */
    private static final class RecordingRequest implements HttpUriRequest {
        final AtomicInteger abortCount = new AtomicInteger();

        @Override
        public void abort() {
            abortCount.incrementAndGet();
        }

        @Override
        public boolean isAborted() {
            return abortCount.get() > 0;
        }

        @Override public String getMethod() {
            throw nope();
        }

        @Override public RequestLine getRequestLine() {
            throw nope();
        }

        @Override public URI getURI() {
            throw nope();
        }

        @Override public ProtocolVersion getProtocolVersion() {
            throw nope();
        }

        @Override public boolean containsHeader(final String name) {
            throw nope();
        }

        @Override public Header[] getHeaders(final String name) {
            throw nope();
        }

        @Override public Header getFirstHeader(final String name) {
            throw nope();
        }

        @Override public Header getLastHeader(final String name) {
            throw nope();
        }

        @Override public Header[] getAllHeaders() {
            throw nope();
        }

        @Override public void addHeader(final Header header) {
            throw nope();
        }

        @Override public void addHeader(final String name, final String value) {
            throw nope();
        }

        @Override public void setHeader(final Header header) {
            throw nope();
        }

        @Override public void setHeader(final String name, final String value) {
            throw nope();
        }

        @Override public void setHeaders(final Header[] headers) {
            throw nope();
        }

        @Override public void removeHeader(final Header header) {
            throw nope();
        }

        @Override public void removeHeaders(final String name) {
            throw nope();
        }

        @Override public org.apache.http.HeaderIterator headerIterator() {
            throw nope();
        }

        @Override public org.apache.http.HeaderIterator headerIterator(final String name) {
            throw nope();
        }

        @SuppressWarnings("deprecation")
        @Override public HttpParams getParams() {
            throw nope();
        }

        @SuppressWarnings("deprecation")
        @Override public void setParams(final HttpParams params) {
            throw nope();
        }

        private static UnsupportedOperationException nope() {
            return new UnsupportedOperationException(
                "RecordingRequest only implements abort()/isAborted(); test should not call this");
        }
    }
}
