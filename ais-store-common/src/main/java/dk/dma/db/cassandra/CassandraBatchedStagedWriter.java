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
