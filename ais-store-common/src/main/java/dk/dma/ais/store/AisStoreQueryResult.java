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
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import dk.dma.ais.packet.AisPacket;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class AisStoreQueryResult implements Iterable<AisPacket> /* , ListenableFuture<Void> */{

    private final AisStoreQueryInnerContext context;

    AisStoreQueryResult(AisStoreQueryInnerContext context) {
        this.context = context;
    }

    /** {@inheritDoc} */
    boolean cancel(boolean mayInterruptIfRunning) {
        return context.inner.cancel(mayInterruptIfRunning);
    }

    /** {@inheritDoc} */
    boolean isCancelled() {
        return context.inner.isCancelled();
    }

    /** {@inheritDoc} */
    boolean isDone() {
        return context.inner.isDone();
    }

    /** {@inheritDoc} */
    Void get() throws InterruptedException, ExecutionException {
        return context.inner.get();
    }

    /** {@inheritDoc} */
    Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return context.inner.get(timeout, unit);
    }

    /** {@inheritDoc} */
    void addListener(Runnable listener, Executor executor) {
        context.inner.addListener(listener, executor);
    }

    // keep

    /** {@inheritDoc} */
    @Override
    public final Iterator<AisPacket> iterator() {
        context.startDate = new Date();
        context.startTime = System.nanoTime();
        return new WrappingIterator(createQuery());
    }

    abstract Iterator<AisPacket> createQuery();

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
            // packets.incrementAndGet();
            return next;
        }

        /** {@inheritDoc} */
        @Override
        public void remove() {
            delegate.remove();
        }
    }
}
