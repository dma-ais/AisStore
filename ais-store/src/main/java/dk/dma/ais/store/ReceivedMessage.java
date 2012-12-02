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
package dk.dma.ais.store;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicReference;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.ISendResultListener;
import dk.dma.ais.reader.SendException;
import dk.dma.ais.reader.SendRequest;
import dk.dma.enav.messaging.MaritimeMessageHandler;
import dk.dma.enav.messaging.MaritimeMessageMetadata;

/**
 * 
 * @author Kasper Nielsen
 */
public class ReceivedMessage {
    private final String stringMessage;
    private volatile AisMessage aisMessage;
    private final long insertTimestamp;

    /**
     * @param stringMessage
     * @param aisMessage
     * @param insertTimestamp
     */
    public ReceivedMessage(String stringMessage, long insertTimestamp) {
        this.stringMessage = requireNonNull(stringMessage);
        this.insertTimestamp = insertTimestamp;
    }

    public String getStringMessage() {
        return stringMessage;
    }

    public static AisMessage fromString(String string) {
        final AtomicReference<AisMessage> ref = new AtomicReference<>();
        if (string != null && string.length() > 0) {
            DummyReader dummy = new DummyReader();

            dummy.registerHandler(new MaritimeMessageHandler<AisMessage>() {

                public void handle(AisMessage message, MaritimeMessageMetadata metadata) {
                    ref.set(message);
                }
            });

            String lines[] = string.split("\\r?\\n");
            for (String s : lines) {
                dummy.handleLine(s);
            }
        }
        return ref.get();
    }

    public static ReceivedMessage from(String message, long received) {
        return new ReceivedMessage(message, received);
    }

    static class DummyReader extends AisReader {
        /** {@inheritDoc} */
        @Override
        protected void handleLine(String line) {
            super.handleLine(line);
        }

        @Override
        public void send(SendRequest sendRequest, ISendResultListener resultListener) throws SendException {}

        @Override
        public Status getStatus() {
            return null;
        }

        @Override
        public void close() {}
    }

    public interface BatchedProducer extends ReceivedMessage.Processor {

        void finished(Throwable exceptionalFinished);
    }

    public interface Processor {
        void process(ReceivedMessage message) throws Exception;
    }

}
