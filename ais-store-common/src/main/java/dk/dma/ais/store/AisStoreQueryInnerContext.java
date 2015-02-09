/* Copyright (c) 2011 Danish Maritime Authority.
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

import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;



import java.util.concurrent.atomic.LongAdder;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.SettableFuture;

/**
 * 
 * @author Kasper Nielsen
 */
class AisStoreQueryInnerContext {
    final LongAdder processedPackets = new LongAdder();
    final LongAdder releasedPackets = new LongAdder();

    final SettableFuture<Void> inner = SettableFuture.create();
    volatile Date startDate;
    volatile Date finishDate;
    volatile long startTime;
    volatile long finishTime;
    final CountDownLatch latch = new CountDownLatch(10);

    final CopyOnWriteArrayList<AbstractIterator<?>> queries = new CopyOnWriteArrayList<>();

    long getTotalProcessed() {

        return processedPackets.sum();
    }

    void finished(AbstractIterator<?> q) {
        queries.remove(q);
        if (queries.isEmpty()) {
            inner.set(null);
        }

        finishTime = System.nanoTime();
        finishDate = new Date();
    }
}
