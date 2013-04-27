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
package dk.dma.db.cassandra;

import static java.util.Objects.requireNonNull;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.mortbay.log.Log;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import dk.dma.commons.service.AbstractBatchedStage;

/**
 * 
 * @author Kasper Nielsen
 */
class CassandraBatchedStagedWriter<T> extends AbstractBatchedStage<T> {

    private final KeySpaceConnection connection;

    private volatile long lastWrite;

    private AtomicLong processed = new AtomicLong();
    private final CassandraWriteSink<T> sink;
    private final long started = System.nanoTime();

    /**
     * @param queueSize
     * @param maxBatchSize
     */
    CassandraBatchedStagedWriter(KeySpaceConnection connection, int batchSize, CassandraWriteSink<T> sink) {
        super(Math.min(100000, batchSize * 100), batchSize);
        this.connection = requireNonNull(connection);
        this.sink = requireNonNull(sink);
    }

    /** {@inheritDoc} */
    @Override
    protected void handleMessages(List<T> messages) {
        MutationBatch m = connection.getKeyspace().prepareMutationBatch();
        for (T t : messages) {
            try {
                sink.process(m, t);
            } catch (RuntimeException e) {
                // Just in case we cannot process a message
                Log.warn("Failed to write message: " + t, e);
            }
        }
        try {
            m.execute();
            sink.onSucces(messages);
            if (lastWrite == 0) {
                System.out
                        .println(new Date() + ": Succesfully wrote first batch with " + processed.get() + " messages");
            }
            if (processed.addAndGet(messages.size()) - lastWrite > 1000000) {
                double time = System.nanoTime() - started;
                double event = processed.get() / time * TimeUnit.MINUTES.toNanos(1);
                DecimalFormat df = new DecimalFormat("#,###,###,##0.00");
                System.out.println(new Date() + ": " + processed.get() + " (" + df.format(event) + " message/second)");
                lastWrite = processed.get();
            }
        } catch (ConnectionException e) {
            sink.onFailure(messages, e);
            try {
                Thread.sleep(5000);// Lets wait for a bit
            } catch (InterruptedException ignore) {
                Thread.interrupted();
            }
        }
    }
}
