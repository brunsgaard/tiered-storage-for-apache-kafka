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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * Tracks in-flight Apache HttpClient requests per worker thread so that a bounded GCS call can be
 * force-cancelled. On operation-timeout (or caller interruption) the canceller thread looks up the
 * worker's in-flight request and calls {@link HttpUriRequest#abort()}, which shuts down the
 * underlying connection and unblocks a worker parked in {@code socketWrite0} — something neither
 * {@code Future.cancel(true)} nor {@code cancel(false)} can do, because {@code Thread.interrupt()}
 * does not unblock the native write syscall.
 *
 * <p>Used only when {@code gcs.http.transport=apache}. All methods are thread-safe.
 */
class AbortableRequestTracker {

    // Currently-executing request per worker thread. Written by the worker (via the interceptor),
    // read and removed by the canceller. Package-private so the race unit test can assert no entry
    // is ever leaked.
    final ConcurrentMap<Thread, HttpUriRequest> activeRequests = new ConcurrentHashMap<>();

    // Workers whose call was cancelled. The interceptor checks this so it can pre-emptively abort
    // any fresh request the SDK's BlobWriteSession chunk-resume logic issues above its own retry
    // layer. Package-private for the same test reason as activeRequests.
    final Set<Thread> cancelledWorkers = ConcurrentHashMap.newKeySet();

    /**
     * Build the request interceptor that registers — or pre-emptively aborts — each outgoing
     * request. Add it via {@code HttpClientBuilder.addInterceptorFirst}.
     */
    HttpRequestInterceptor interceptor() {
        return (request, ctx) -> {
            // Apache wraps requests in HttpRequestWrapper before the interceptor sees them, and
            // HttpRequestWrapper.abort() throws UnsupportedOperationException — only the original
            // HttpRequestBase has a working abort() that shuts down the connection. Unwrap to it.
            final HttpUriRequest realRequest;
            if (request instanceof HttpRequestWrapper) {
                final HttpRequest original = ((HttpRequestWrapper) request).getOriginal();
                if (!(original instanceof HttpUriRequest)) {
                    return;
                }
                realRequest = (HttpUriRequest) original;
            } else if (request instanceof HttpUriRequest) {
                realRequest = (HttpUriRequest) request;
            } else {
                return;
            }
            registerOrAbort(realRequest);
        };
    }

    /**
     * Register an in-flight request against the issuing worker thread, or abort it immediately if
     * that worker has already been cancelled.
     *
     * <p>The naive "check cancelled, then put" ordering races: the canceller's two writes
     * ({@code cancelledWorkers.add} then {@code activeRequests.remove}) can land entirely between
     * the check ([A]) and the put ([B]), leaving the request mapped but never aborted — the worker
     * then parks in {@code socketWrite0} and leaks. The re-check at [C] closes that window.
     */
    void registerOrAbort(final HttpUriRequest realRequest) {
        final Thread current = Thread.currentThread();
        // [A] Fast path: worker was already cancelled before this request reached the interceptor
        // (e.g. BlobWriteSession's chunk-resume issuing a fresh PUT after the cancel).
        if (cancelledWorkers.contains(current)) {
            realRequest.abort();
            return;
        }
        // [B] Register so the canceller can find us by thread.
        activeRequests.put(current, realRequest);
        afterRegister(current);
        // [C] Re-check: if the canceller's add raced in after [A], its remove returned null (we
        // hadn't put yet) and skipped the abort, so we catch it here. Idempotent — a double abort()
        // is a no-op on HttpRequestBase.
        if (cancelledWorkers.contains(current)) {
            activeRequests.remove(current);
            realRequest.abort();
        }
    }

    /**
     * Test seam fired between the [B] put and the [C] re-check. A no-op in production; the race
     * unit test overrides it to deterministically simulate the canceller landing in that window,
     * reproducing an interleave that would otherwise only appear under timing pressure.
     */
    void afterRegister(final Thread current) {
    }

    /**
     * Mark a worker cancelled and force-close its in-flight request. Safe no-op when {@code worker}
     * is null or has no request currently mapped (e.g. it has not yet reached the interceptor).
     */
    void cancelAndAbort(final Thread worker) {
        if (worker == null) {
            return;
        }
        cancelledWorkers.add(worker);
        final HttpUriRequest req = activeRequests.remove(worker);
        if (req != null) {
            req.abort();
        }
    }

    /**
     * Clear all per-worker state for {@code worker}. Called from the bounded call's {@code finally}
     * so the maps don't grow and a reused worker thread does not inherit a stale cancelled flag.
     */
    void clear(final Thread worker) {
        activeRequests.remove(worker);
        cancelledWorkers.remove(worker);
    }
}
