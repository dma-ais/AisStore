/* Copyright (c) 2011 Danish Maritime Authority
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
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
