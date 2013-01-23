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
package dk.dma.app.cassandra;

import static java.util.Objects.requireNonNull;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import dk.dma.commons.service.AbstractBatchedStage;

/**
 * 
 * @author Kasper Nielsen
 */
class CassandraBatchedStagedWriter<T> extends AbstractBatchedStage<T> {

    private final KeySpaceConnection connection;

    private final CassandraWriteSink<T> sink;

    private volatile long lastWrite;
    private final long started = System.nanoTime();
    private AtomicLong processed = new AtomicLong();

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
            sink.process(m, t);
        }
        try {
            m.execute();
            sink.onSucces(messages);
            if (processed.addAndGet(messages.size()) - lastWrite > 100000) {
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
