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
