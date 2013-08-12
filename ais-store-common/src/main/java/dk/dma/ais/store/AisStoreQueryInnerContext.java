/*
 * Copyright (c) 2008 Kasper Nielsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.dma.ais.store;

import static java.util.Objects.requireNonNull;

import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;

import jsr166e.LongAdder;

import com.google.common.util.concurrent.SettableFuture;

/**
 * 
 * @author Kasper Nielsen
 */
class AisStoreQueryInnerContext {
    final LongAdder processedPackets = new LongAdder();
    final LongAdder releasedPackets = new LongAdder();

    final SettableFuture<Void> inner = SettableFuture.create();
    final CopyOnWriteArrayList<PerReader> readers = new CopyOnWriteArrayList<>();
    volatile Date startDate;
    volatile Date finishDate;
    volatile long startTime;
    volatile long finishTime;

    long getTotalProcessed() {
        return processedPackets.sum();
    }

    void finished() {
        finishTime = System.nanoTime();
        finishDate = new Date();
        inner.set(null);
    }

    static class PerReader {
        final AisStoreQueryInnerContext context;

        PerReader(AisStoreQueryInnerContext context) {
            this.context = requireNonNull(context);
        }

        volatile long latestDateProcessed;

        void finished() {

        }
    }
}
