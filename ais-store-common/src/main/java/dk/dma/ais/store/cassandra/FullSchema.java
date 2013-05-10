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
import dk.dma.ais.message.IPositionMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.commons.tracker.PositionTracker;
import dk.dma.db.cassandra.CassandraWriteSink;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;

/**
 * 
 * @author Kasper Nielsen
 */
public class FullSchema extends CassandraWriteSink<AisPacket> {

    public static final ColumnFamily<Integer, byte[]> MESSAGES_CELL1 = new ColumnFamily<>("messages_cell1",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, byte[]> MESSAGES_CELL10 = new ColumnFamily<>("messages_cell10",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, byte[]> MESSAGES_MMSI = new ColumnFamily<>("messages_mmsi",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, byte[]> MESSAGES_TIME = new ColumnFamily<>("messages_time",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public static final ColumnFamily<Integer, String> MMSI = new ColumnFamily<>("mmsi", IntegerSerializer.get(),
            StringSerializer.get());

    /**
     * The duration in milliseconds from when the latest positional message is received for a specific mmsi number is
     * still valid.
     */
    public static final long POSITION_TIMEOUT_MS = 20 * 60 * 1000; // 20 min

    public static final ColumnFamily<String, byte[]> POSITIONS = new ColumnFamily<>("positions",
            StringSerializer.get(), BytesArraySerializer.get());

    private final PositionTracker<Integer> tracker = new PositionTracker<>();

    void messagesCell1(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        Position p = null;
        if (message instanceof IPositionMessage) {
            p = ((IPositionMessage) message).getPos().getGeoLocation();
            // Update latest position for later use
            tracker.update(message.getUserId(), PositionTime.create(p, packet.getBestTimestamp()));
        } else if (message != null) {
            int mmsi = message.getUserId();
            PositionTime latest = tracker.getLatest(mmsi);
            if (packet.getBestTimestamp() - latest.getTime() < POSITION_TIMEOUT_MS) {
                p = latest;
            }
        }

        // Only update if the position is not null
        if (p != null) {
            int cell1 = (int) p.getCell(1); // around 64800 total cells
            ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_CELL1, cell1).setTimestamp(ts);
            byte[] column = Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128());
            r.putColumn(column, packet.toByteArray());
        }
    }

    /**
     * Saves the packet order by timestamp. Each mmsi number corresponds to one row. The columns are ordered by
     * timestamp concatenated with a hash of the message.
     */
    void messagesMmsi(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
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
    void messagesTime(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_TIME, TimeFormatter.MIN10.getAsInt(ts)).setTimestamp(ts);
        // We only store the 10 minute remainder of the timestamp. We can always figure out the right timestamp by
        // doing rowId*10*60*1000 + columnId (minus the 128 bit hash)
        byte[] column = Bytes.concat(Ints.toByteArray(TimeFormatter.MIN10.getReminderAsInt(ts)),
                packet.calculateHash128());
        r.putColumn(column, packet.toByteArray());
    }

    void mmsi(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message != null) {
            ColumnListMutation<String> r = mb.withRow(MMSI, message.getUserId()).setTimestamp(ts);
            r.putColumn("last_message", packet.getStringMessage());
            r.putColumn("last_message_timestamp", ts);
            if (message instanceof IPositionMessage) {
                IPositionMessage m = (IPositionMessage) message;
                Position p = m.getPos().getGeoLocation();
                if (p != null) {
                    r.putColumn("last_position_message", packet.getStringMessage());
                    r.putColumn("last_position_timestamp", ts);
                    r.putColumn("last_position_timehour", TimeUtil.hoursSinceEpoch(ts));
                    r.putColumn("last_position_timeminute", TimeUtil.minutesSinceEpoch(ts));
                    r.putColumn("last_position_timesecond", TimeUtil.secondsSinceEpoch(ts));
                    r.putColumn("last_position_cell001", p.getCell(0.01));
                    r.putColumn("last_position_cell01", p.getCell(0.1));
                    r.putColumn("last_position_cell1", p.getCell(1));
                }
            }
        }
    }

    void positions(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message instanceof IPositionMessage) {
            IPositionMessage m = (IPositionMessage) message;
            Position p = m.getPos().getGeoLocation();
            if (p != null) {
                byte[] userid = Ints.toByteArray(message.getUserId());
                int cell10 = (int) p.getCell(10); // around 648 total cells
                int cell1 = (int) p.getCell(1); // around 64800 total cells
                int hour = TimeUtil.hoursSinceEpoch(ts);
                int days = TimeUtil.daysSinceEpoch(ts);
                int minutes = TimeUtil.minutesSinceEpoch(ts);

                long position = p.toPackedLong();
                ColumnListMutation<byte[]> r;
                // ---------------- Ship
                // Positions for a ship
                r = mb.withRow(POSITIONS, message.getUserId() + "_hour").setTimestamp(ts);
                r.putColumn(Ints.toByteArray(hour), position);

                r = mb.withRow(POSITIONS, message.getUserId() + "_minute").setTimestamp(ts);
                r.putColumn(Ints.toByteArray(minutes), position);

                // cell
                r = mb.withRow(POSITIONS, message.getUserId() + "_cell1").setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(hour), Ints.toByteArray(cell1)), position);

                r = mb.withRow(POSITIONS, message.getUserId() + "_cell10").setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(hour), Ints.toByteArray(cell10)), position);

                // ---------------- ALL
                // position
                r = mb.withRow(POSITIONS, "positions_day100_day" + TimeFormatter.DAY100.get(ts)).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(days), userid), position);

                r = mb.withRow(POSITIONS, "positions_day_hour" + TimeFormatter.DAY.get(ts)).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(hour), userid), position);

                r = mb.withRow(POSITIONS, "positions_hour_minutes" + hour).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(minutes), userid), position);

                // cell
                r = mb.withRow(POSITIONS, "cell1_" + hour).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(cell10), Ints.toByteArray(cell1), userid), position);

                r = mb.withRow(POSITIONS, "cell10_" + hour).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(cell10), userid), position);

            }
        }
    }

    public void process(MutationBatch mb, AisPacket packet) {
        AisMessage message = packet.tryGetAisMessage();
        long ts = packet.getTimestamp().getTime();

        messagesTime(mb, packet, message, ts);
        messagesMmsi(mb, packet, message, ts);
        messagesCell1(mb, packet, message, ts);
        positions(mb, packet, message, ts);
        // mmsi(mb, packet, message, ts);
    }

}
