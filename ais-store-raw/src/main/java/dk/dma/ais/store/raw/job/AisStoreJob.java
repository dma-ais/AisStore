/* Copyright (c) 2011 Danish Maritime Authority.
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
package dk.dma.ais.store.raw.job;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
// et midlertidig job
public abstract class AisStoreJob {
    // ship timeout

    <T extends AisMessage> T getLatestMessage(Class<T> messageType) {
        return null;
    }

    <T extends AisMessage> AisPacket getLatestPacket(Class<T> messageType) {
        return null;
    }

    Position getLatestPosition() {
        return null;
    }

    abstract void process(AisPacket p);
}
