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

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_AISDATA;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_AISDATA_DIGEST;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_CELLID;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_MMSI;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMEBLOCK;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMESTAMP;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_AREA_CELL1;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_AREA_CELL10;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_AREA_UNKNOWN;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_MMSI;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_TIME;
import static dk.dma.ais.store.AisStoreSchema.getDigest;
import static dk.dma.ais.store.AisStoreSchema.timeBlock;

/**
 * The schema used in AisStore.
 * 
 * @author Kasper Nielsen
 */
public abstract class DefaultAisStoreWriter extends CassandraBatchedStagedWriter<AisPacket> {

    static final Logger LOG = LoggerFactory.getLogger(DefaultAisStoreWriter.class);

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
        Objects.requireNonNull(batch);
        Objects.requireNonNull(packet);

        final long millisSinceEpoch = packet.getBestTimestamp();
        if (millisSinceEpoch <= 0) {
            LOG.warn("Invalid timestamp in packet: " + packet.getStringMessage());
        }

        AisMessage message = packet.tryGetAisMessage();
        if (message == null) {
            LOG.warn("Cannot decode packet (to obtain MMSI): " + packet.getStringMessage());
        }

        // We need to calc these values only once per packet.
        final int mmsi = message == null ? -1 : message.getUserId();
        final Instant timestamp = Instant.ofEpochMilli(millisSinceEpoch);
        final Position position = getPosition(packet);
        final byte[] digest = getDigest(packet);
        final String rawMessage = packet.getStringMessage();

        // Store packets in Cassandra
        if (millisSinceEpoch > 0)
            storeByTime(batch, timestamp, digest, rawMessage); // Store packet by time

        if (mmsi > 0)
            storeByMmsi(batch, timestamp, mmsi, digest, rawMessage); // Store packet by mmsi

        if (millisSinceEpoch > 0 && mmsi > 0)
            storeByArea(batch, timestamp, mmsi, position, digest, rawMessage); // Store packet by area
    }

    /** Stores the specified packet by position (area). */
    private static void storeByArea(List<RegularStatement> batch, Instant timestamp, int mmsi, Position p, byte[] digest, String rawMessage) {
        if (p == null) {
            // Okay we have no idea of the position of the ship. Store it in this table and process it later.
            Insert i = QueryBuilder.insertInto(TABLE_PACKETS_AREA_UNKNOWN.toString());
            i.value(COLUMN_MMSI.toString(), mmsi);
            i.value(COLUMN_TIMEBLOCK.toString(), timeBlock(TABLE_PACKETS_AREA_UNKNOWN, timestamp));
            i.value(COLUMN_TIMESTAMP.toString(), timestamp.toEpochMilli());
            i.value(COLUMN_AISDATA_DIGEST.toString(), ByteBuffer.wrap(digest));
            i.value(COLUMN_AISDATA.toString(), rawMessage);
            batch.add(i);
        } else {
            // Cells with size 1 degree
            Insert i = QueryBuilder.insertInto(TABLE_PACKETS_AREA_CELL1.toString());
            i.value(COLUMN_CELLID.toString(), p.getCellInt(1));
            i.value(COLUMN_TIMEBLOCK.toString(), timeBlock(TABLE_PACKETS_AREA_CELL1, timestamp));
            i.value(COLUMN_TIMESTAMP.toString(), timestamp.toEpochMilli());
            i.value(COLUMN_AISDATA_DIGEST.toString(), ByteBuffer.wrap(digest));
            i.value(COLUMN_AISDATA.toString(), rawMessage);
            batch.add(i);

            // Cells with size 10 degree
            i = QueryBuilder.insertInto(TABLE_PACKETS_AREA_CELL10.toString());
            i.value(COLUMN_CELLID.toString(), p.getCellInt(10));
            i.value(COLUMN_TIMEBLOCK.toString(), timeBlock(TABLE_PACKETS_AREA_CELL10, timestamp));
            i.value(COLUMN_TIMESTAMP.toString(), timestamp.toEpochMilli());
            i.value(COLUMN_AISDATA_DIGEST.toString(), ByteBuffer.wrap(digest));
            i.value(COLUMN_AISDATA.toString(), rawMessage);
            batch.add(i);
        }
    }

    /** Stores the specified packet by MMSI. */
    private static void storeByMmsi(List<RegularStatement> batch, Instant timestamp, int mmsi, byte[] digest, String rawMessage) {
        Insert i = QueryBuilder.insertInto(TABLE_PACKETS_MMSI.toString());
        i.value(COLUMN_MMSI.toString(), mmsi);
        i.value(COLUMN_TIMEBLOCK.toString(), timeBlock(TABLE_PACKETS_MMSI, timestamp));
        i.value(COLUMN_TIMESTAMP.toString(), timestamp.toEpochMilli());
        i.value(COLUMN_AISDATA_DIGEST.toString(), ByteBuffer.wrap(digest));
        i.value(COLUMN_AISDATA.toString(), rawMessage);
        batch.add(i);
    }

    /** Stores the specified packet by time. */
    private static void storeByTime(List<RegularStatement> batch, Instant timestamp, byte[] digest, String rawMessage) {
        Insert i = QueryBuilder.insertInto(TABLE_PACKETS_TIME.toString());
        i.value(COLUMN_TIMEBLOCK.toString(), timeBlock(TABLE_PACKETS_TIME, timestamp));
        i.value(COLUMN_TIMESTAMP.toString(), timestamp.toEpochMilli());
        i.value(COLUMN_AISDATA_DIGEST.toString(), ByteBuffer.wrap(digest));
        i.value(COLUMN_AISDATA.toString(), rawMessage);
        batch.add(i);
    }

    private Position getPosition(AisPacket packet) {
        Position p = null;

        final AisMessage message = packet.tryGetAisMessage();
        if (message != null) {
            final int mmsi = message.getUserId();
            final long timestamp = packet.getBestTimestamp();

            p = message.getValidPosition();

            if (p == null) { // Try to find an estimated position
                // Use the last received position message unless the position has timed out (POSITION_TIMEOUT_MS)
                p = tracker.asMap().getOrDefault(message.getUserId(), null);
            } else { // Update the tracker with latest position
                //but only update the tracker IF the new time is better
                tracker.asMap().merge(message.getUserId(), p.withTime(timestamp), (a, b) -> a.getTime() > b.getTime() ? a : b);
            }
        }

        return p;
    }

}
