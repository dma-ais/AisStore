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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import dk.dma.ais.reader.AisReader;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class CassandraWriteSink<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AisReader.class);

    private final AtomicLong written = new AtomicLong();

    public abstract void process(MutationBatch b, T message);

    public void onSucces(List<T> messages) {
        written.addAndGet(messages.size());
    }

    public void onFailure(List<T> messages, ConnectionException cause) {
        LOG.error("Could not persist messages in cassandra", cause);
    }
}
