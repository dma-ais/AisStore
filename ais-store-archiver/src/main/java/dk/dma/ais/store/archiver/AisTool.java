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
package dk.dma.ais.store.archiver;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.reader.AisReader;
import dk.dma.enav.util.function.Consumer;

/**
 * 
 * @author Kasper Nielsen
 */
class AisTool {

    static Service wrapAisReader(final AisReader reader, final Consumer<AisPacket> handler, final CountDownLatch latch) {
        requireNonNull(reader);
        requireNonNull(handler);
        requireNonNull(latch);
        reader.registerPacketHandler(new Consumer<AisPacket>() {

            @Override
            public void accept(AisPacket aisPacket) {
                handler.accept(aisPacket);
            }
        });
        return new AbstractExecutionThreadService() {

            @Override
            protected void run() throws Exception {
                try {
                    reader.run();
                } finally {
                    latch.countDown();
                }
            }
        };
    }

    static Service wrapAisReader(final AisReader reader, final Consumer<AisPacket> handler) {
        requireNonNull(reader);
        requireNonNull(handler);
        reader.registerPacketHandler(new Consumer<AisPacket>() {

            @Override
            public void accept(AisPacket aisPacket) {
                handler.accept(aisPacket);
            }
        });
        return new AbstractExecutionThreadService() {

            @Override
            protected void run() throws Exception {
                reader.run();
            }
        };
    }
}
