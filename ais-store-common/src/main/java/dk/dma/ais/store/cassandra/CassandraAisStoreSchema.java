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
package dk.dma.ais.store.cassandra;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisPosition;
import dk.dma.ais.message.AisStaticCommon;
import dk.dma.ais.message.IPositionMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.cassandra.support.CassandraWriteSink;
import dk.dma.commons.tracker.PositionTracker;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class CassandraAisStoreSchema extends CassandraWriteSink<AisPacket> {

    public static final ColumnFamily<Integer, byte[]> MESSAGES_CELL1 = new ColumnFamily<>("messages_cell1",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, byte[]> MESSAGES_CELL10 = new ColumnFamily<>("messages_cell10",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, byte[]> MESSAGES_MMSI = new ColumnFamily<>("messages_mmsi",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, byte[]> MESSAGES_TIME = new ColumnFamily<>("messages_time",
            IntegerSerializer.get(), BytesArraySerializer.get());

    /**
     * The duration in milliseconds from when the latest positional message is received for a specific mmsi number is
     * still valid.
     */
    // TODO different for sat packets???
    public static final long POSITION_TIMEOUT_MS = 20 * 60 * 1000; // 20 min

    public static final ColumnFamily<String, byte[]> POSITIONS_LATEST = new ColumnFamily<>("latest_data",
            StringSerializer.get(), BytesArraySerializer.get());

    private final PositionTracker<Integer> tracker = new PositionTracker<>();

    public void process(MutationBatch mb, AisPacket packet) {
        AisMessage message = packet.tryGetAisMessage();
        long ts = packet.getTimestamp().getTime();

        // Store message / table
        tableTime(mb, packet, message, ts);
        if (message == null) {
            return; // all tables are only stored if the message is non null
        }

        // extract position
        Position p = null;
        if (message instanceof IPositionMessage) {
            AisPosition p1 = ((IPositionMessage) message).getPos();
            if (p1 != null) {
                p = p1.getGeoLocation();
                if (p != null) {
                    // lets update the tracker with lates position
                    tracker.update(message.getUserId(), PositionTime.create(p, packet.getBestTimestamp()));
                }
            }
        }
        if (message != null) {
            tableMmsi(mb, packet, message, ts);
            tableCell1_10(mb, packet, message, p, ts);
            // tableLastestPosition(mb, packet, message, p, ts);
        }
        // positions(mb, packet, message, ts);
        // mmsi(mb, packet, message, ts);
    }

    /**
     * Stores the latest position packet. It is stored twice. Once in the row named ANY_SOURCE and once in the row named
     * SOURCE_$SOURCE_ID$. For both rows the mmsi number is used as the name of the column.
     */
    void tableLastestPosition(MutationBatch mb, AisPacket packet, AisMessage message, Position p, long ts) {
        String sourceId = packet.getTags().getSourceId();
        byte[] mmsi = Ints.toByteArray(message.getUserId());
        byte[] pp = packet.toByteArray();

        if (message instanceof AisStaticCommon) {
            mb.withRow(POSITIONS_LATEST, "STA_ANY_SOURCE").setTimestamp(ts).putColumn(mmsi, pp);
            if (sourceId != null) { // if source is non-null also store it
                mb.withRow(POSITIONS_LATEST, "STA_SOURCE_" + sourceId).setTimestamp(ts).putColumn(mmsi, pp);
            }
        }
        if (p != null) {
            mb.withRow(POSITIONS_LATEST, "POS_ANY_SOURCE").setTimestamp(ts).putColumn(mmsi, pp);
            if (sourceId != null) { // if source is non-null also store it
                mb.withRow(POSITIONS_LATEST, "POS_SOURCE_" + sourceId).setTimestamp(ts).putColumn(mmsi, pp);
            }
        }
    }

    void tableCell1_10(MutationBatch mb, AisPacket packet, AisMessage message, Position p, long ts) {
        // Try to an estimated position if there was no position in the message
        if (p == null) {
            // Not a position message. Store the message in the same cell as the last received position message
            // unless the position has timed out (POSITION_TIMEOUT_MS) or no position has ever been received
            int mmsi = message.getUserId();
            PositionTime latest = tracker.getLatest(mmsi);
            if (latest != null && packet.getBestTimestamp() - latest.getTime() < POSITION_TIMEOUT_MS) {
                p = latest;
            }
        }

        // Only update if the position is not null
        if (p != null) {
            byte[] column = Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128());

            // Cells with size 1 degree
            ColumnListMutation<byte[]> m1 = mb.withRow(MESSAGES_CELL1, (int) p.getCell(1)).setTimestamp(ts);
            m1.putColumn(column, packet.toByteArray());

            // Cells with size 10 degree
            ColumnListMutation<byte[]> m10 = mb.withRow(MESSAGES_CELL10, (int) p.getCell(10)).setTimestamp(ts);
            m10.putColumn(column, packet.toByteArray());
        }
    }

    /**
     * Saves the packet order by timestamp. Each mmsi number corresponds to one row. The columns are ordered by
     * timestamp concatenated with a hash of the message.
     */
    void tableMmsi(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message != null) {
            ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_MMSI, message.getUserId()).setTimestamp(ts);
            byte[] column = Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128());
            r.putColumn(column, packet.toByteArray());
        }
    }

    /**
     * Save the packet ordered by time. Each row has an int key which indicates the number of 10 minute blocks since the
     * epoch. Within each 10 minute blocks, packets are store ordered by timestamp concatenated with a hash of the
     * message.
     */
    void tableTime(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_TIME, TimeFormatter.MIN10.getAsInt(ts)).setTimestamp(ts);
        // We only store the 10 minute remainder of the timestamp. We can always figure out the right timestamp by
        // doing rowId*10*60*1000 + columnId (minus the 128 bit hash)
        byte[] column = Bytes.concat(Ints.toByteArray(TimeFormatter.MIN10.getReminderAsInt(ts)),
                packet.calculateHash128());
        r.putColumn(column, packet.toByteArray());
    }
}
