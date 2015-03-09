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

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;
import dk.dma.ais.packet.AisPacket;
import dk.dma.commons.util.Iterators;
import dk.dma.db.cassandra.CassandraQuery;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisStoreQueryResult extends CassandraQuery implements Iterable<AisPacket>, ListenableFuture<Void> {

    private final AisStoreQueryInnerContext context;

    private final Object lock = new Object();

    private final List<AbstractIterator<AisPacket>> queries = new ArrayList<>();

    private Iterator<AisPacket> iterator;

    final AtomicLong releasedPackets = new AtomicLong();

    AisStoreQueryResult(AisStoreQueryInnerContext context, List<AbstractIterator<AisPacket>> queries) {
        this.context = context;
        this.queries.addAll(queries);
    }

    public String getState() {
        if (!isDone()) {
            return "Running";
        }
        return isCancelled() ? "Cancelled" : "Done";
    }

    /** {@inheritDoc} */
    public boolean cancel(boolean mayInterruptIfRunning) {
        return context.inner.cancel(mayInterruptIfRunning);
    }

    /** {@inheritDoc} */
    public boolean isCancelled() {
        return context.inner.isCancelled();
    }

    /** {@inheritDoc} */
    public boolean isDone() {
        return context.inner.isDone();
    }

    /** {@inheritDoc} */
    public Void get() throws InterruptedException, ExecutionException {
        return context.inner.get();
    }

    /** {@inheritDoc} */
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return context.inner.get(timeout, unit);
    }

    /** {@inheritDoc} */
    public void addListener(Runnable listener, Executor executor) {
        context.inner.addListener(listener, executor);
    }

    /** {@inheritDoc} */
    @Override
    public final Iterator<AisPacket> iterator() {
        synchronized (lock) {
            Iterator<AisPacket> iterator = this.iterator;
            if (iterator == null) {
                context.startDate = new Date();
                context.startTime = System.nanoTime();
                if (queries.size() == 1) {
                    iterator = queries.get(0);
                } else {
                    iterator = Iterators.combine(queries, AisStoreQuery.COMPARATOR);
                }
                return this.iterator = new WrappingIterator(iterator);
            }
            return iterator;
        }
    }

    /**
     * Returns the number of packets that have been returned so far.
     * 
     * @return the number of packets that have been returned so far
     */
    long getNumberOfProcessedPackets() {
        return 0;// context.processedPackets.get();
    }

    long getDuration() {
        return 3;
    }

    class WrappingIterator implements Iterator<AisPacket> {
        final Iterator<AisPacket> delegate;

        WrappingIterator(Iterator<AisPacket> delegate) {
            this.delegate = requireNonNull(delegate);
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        /** {@inheritDoc} */
        @Override
        public AisPacket next() {
            AisPacket next = delegate.next();
            releasedPackets.incrementAndGet();
            return next;
        }

        /** {@inheritDoc} */
        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }
}
