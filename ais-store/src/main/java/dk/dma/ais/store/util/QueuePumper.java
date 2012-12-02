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
package dk.dma.ais.store.util;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import dk.dma.ais.store.query.ReceivedMessage;

/**
 * 
 * @author Kasper Nielsen
 */
public class QueuePumper implements Callable<Void>, ReceivedMessage.BatchedProducer {
    private final static ReceivedMessage SHUTDOWN = ReceivedMessage.from("foo", -1);

    private final ReceivedMessage.Processor batchedProducer;

    private final BlockingQueue<ReceivedMessage> consumer;

    private volatile Throwable exception;

    private volatile BlockingQueue<ReceivedMessage> producer;

    public QueuePumper(ReceivedMessage.Processor batchedProducer, int bufferSize) {
        this.batchedProducer = requireNonNull(batchedProducer);
        consumer = producer = new ArrayBlockingQueue<>(bufferSize);
    }

    /** {@inheritDoc} */
    @Override
    public Void call() throws Exception {
        for (;;) {
            ReceivedMessage take = consumer.take();
            if (take == SHUTDOWN) {
                return null;
            }
            try {
                batchedProducer.process(take);
            } catch (Exception | Error e) {
                consumer.clear();// Clear messages
                producer = null;
                exception = e;
                throw e;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void finished(Throwable exceptionalFinished) {
        this.exception = exceptionalFinished;
        producer.add(SHUTDOWN);
        producer = null;
    }

    /** {@inheritDoc} */
    @Override
    public void process(ReceivedMessage message) throws Exception {
        BlockingQueue<ReceivedMessage> producer = this.producer;
        if (producer != null) {
            producer.put(message);
        } else {
            throw new IllegalStateException("Consumer side has been shutdown", exception);
        }
    }

}
