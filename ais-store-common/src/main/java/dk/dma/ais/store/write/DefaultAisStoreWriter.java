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
package dk.dma.ais.store.write;

import static dk.dma.ais.store.AisStoreSchema.storeByArea;
import static dk.dma.ais.store.AisStoreSchema.storeByMmsi;
import static dk.dma.ais.store.AisStoreSchema.storeByTime;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.RegularStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;

/**
 * The schema used in AisStore.
 * 
 * @author Kasper Nielsen
 */
public abstract class DefaultAisStoreWriter extends CassandraBatchedStagedWriter<AisPacket> {

    /**
     * The duration in milliseconds from when the latest positional message is received for a specific mmsi number is
     * still valid.
     */
    // TODO different for sat packets???
    public static final long POSITION_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

    /** A position tracker used to keeping an eye on previously received messages. */
    private final Cache<Integer,PositionTime> tracker = CacheBuilder
            .newBuilder()
            .expireAfterWrite(POSITION_TIMEOUT_MS,TimeUnit.MILLISECONDS)
            .build();        


    /**
     * @param connection
     * @param batchSize
     */
    public DefaultAisStoreWriter(CassandraConnection connection, int batchSize) {
        super(connection, batchSize);
        
    }

    public void handleMessage(List<RegularStatement> batch, AisPacket packet) {
        long ts = packet.getBestTimestamp();
        if (ts > 0) { // only save packets with a valid timestamp

            byte[] hash = Hashing.murmur3_128().hashUnencodedChars(packet.getStringMessage()).asBytes();

            byte[] column = Bytes.concat(Longs.toByteArray(ts), hash); // the column
            byte[] data = packet.toByteArray(); // the serialized packet

            storeByTime(batch, ts, column, data); // Store packet by time

            // packets are only stored by time, if they are not a proper message
            AisMessage message = packet.tryGetAisMessage();
            if (message == null) {
                return;
            }

            storeByMmsi(batch, ts, column, message.getUserId(), data); // Store packet by mmsi

            Position p = message.getValidPosition();
                        
            if (p == null) { // Try to find an estimated position
                // Use the last received position message unless the position has timed out (POSITION_TIMEOUT_MS)
                p = tracker.asMap().getOrDefault(message.getUserId(),null);                
            } else { // Update the tracker with latest position
                
                //but only update the tracker IF the new time is better
                tracker.asMap().merge(message.getUserId(), p.withTime(ts), (a,b) -> {
                    return a.getTime() > b.getTime() ? a : b ;
                });
            }
            storeByArea(batch, ts, column, message.getUserId(), p, data);
        }
    }
}
