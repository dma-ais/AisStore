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
package dk.dma.ais.store.cassandra.support;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import dk.dma.commons.service.AbstractBatchedStage;

/**
 * 
 * @author Kasper Nielsen
 */
class CassandraBatchedStagedWriter<T> extends AbstractBatchedStage<T> {

    /** The logger. */
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBatchedStagedWriter.class);

    /** The connection to Cassandra. */
    private final KeySpaceConnection connection;

    private final CassandraWriteSink<T> sink;

    final MetricRegistry metrics = new MetricRegistry();

    final Meter persistedCount = metrics.meter(MetricRegistry.name("aistore", "cassandra",
            "Number of persisted AIS messages"));

    /**
     * @param queueSize
     * @param maxBatchSize
     */
    CassandraBatchedStagedWriter(KeySpaceConnection connection, int batchSize, CassandraWriteSink<T> sink) {
        super(Math.min(100000, batchSize * 100), batchSize);
        this.connection = requireNonNull(connection);
        this.sink = requireNonNull(sink);

        final JmxReporter reporter = JmxReporter.forRegistry(metrics).inDomain("fooo.erer.er").build();

        reporter.start();
    }

    /** {@inheritDoc} */
    @Override
    protected void handleMessages(List<T> messages) {
        // Create a batch of message that we want to write.
        MutationBatch m = connection.getKeyspace().prepareMutationBatch();
        for (T t : messages) {
            try {
                sink.process(m, t);
            } catch (RuntimeException e) {
                LOG.warn("Failed to write message: " + t, e); // Just in case we cannot process a message
            }
        }

        // Try writing the batch
        try {
            m.execute();
            persistedCount.mark(messages.size());
            sink.onSucces(messages);
        } catch (ConnectionException e) {
            sink.onFailure(messages, e);
            try {
                Thread.sleep(2500);// Lets wait for a bit
            } catch (InterruptedException ignore) {
                Thread.interrupted();
            }
        }
    }
}
