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
package dk.dma.ais.store.cassandra;

import static com.google.common.primitives.Bytes.concat;

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
import dk.dma.ais.store.util.TimeFormatter;
import dk.dma.ais.store.util.TimeUtil;
import dk.dma.app.cassandra.CassandraWriteSink;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
public class FullSchema extends CassandraWriteSink<AisPacket> {

    public static final FullSchema INSTANCE = new FullSchema();

    public final static ColumnFamily<Integer, byte[]> MESSAGES_CELL1 = new ColumnFamily<>("messages_cell1",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public final static ColumnFamily<Integer, byte[]> MESSAGES_CELL10 = new ColumnFamily<>("messages_cell10",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public final static ColumnFamily<Integer, byte[]> MESSAGES_MMSI = new ColumnFamily<>("messages_mmsi",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public final static ColumnFamily<Integer, byte[]> MESSAGES_TIME = new ColumnFamily<>("messages_time",
            IntegerSerializer.get(), BytesArraySerializer.get());

    public final static ColumnFamily<Integer, String> MMSI = new ColumnFamily<>("mmsi", IntegerSerializer.get(),
            StringSerializer.get());

    public final static ColumnFamily<String, byte[]> POSITIONS = new ColumnFamily<>("positions",
            StringSerializer.get(), BytesArraySerializer.get());

    void messagesCell1(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message instanceof IPositionMessage) {
            IPositionMessage m = (IPositionMessage) message;
            Position p = m.getPos().getGeoLocation();
            if (p != null) {
                int cell1 = (int) p.getCell(1); // around 64800 total cells
                ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_CELL1, cell1).setTimestamp(ts);
                byte[] column = Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128());
                r.putColumn(column, packet.toByteArray());
            }
        }
    }

    void messagesCell10(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message instanceof IPositionMessage) {
            IPositionMessage m = (IPositionMessage) message;
            Position p = m.getPos().getGeoLocation();
            if (p != null) {
                int cell10 = (int) p.getCell(10); // around 648 total cells
                int c = cell10 << 22 + TimeUtil.daysSinceEpoch(ts);
                ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_CELL10, c).setTimestamp(ts);
                byte[] column = Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128());
                r.putColumn(column, packet.toByteArray());
            }
        }
    }

    void messagesMmsi(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message != null) {
            ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_MMSI, message.getUserId()).setTimestamp(ts);
            byte[] column = Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128());
            r.putColumn(column, packet.toByteArray());
        }
    }

    public static void main(String[] args) {
        System.out.println(TimeFormatter.MIN10.getAsInt(System.currentTimeMillis()));
    }

    void messagesTime(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        ColumnListMutation<byte[]> r = mb.withRow(MESSAGES_TIME, TimeFormatter.MIN10.getAsInt(ts)).setTimestamp(ts);
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
                r.putColumn(concat(Ints.toByteArray(hour), Ints.toByteArray(cell1)), position);

                r = mb.withRow(POSITIONS, message.getUserId() + "_cell10").setTimestamp(ts);
                r.putColumn(concat(Ints.toByteArray(hour), Ints.toByteArray(cell10)), position);

                // ---------------- ALL
                // position
                r = mb.withRow(POSITIONS, "positions_day100_day" + TimeFormatter.DAY100.get(ts)).setTimestamp(ts);
                r.putColumn(concat(Ints.toByteArray(days), userid), position);

                r = mb.withRow(POSITIONS, "positions_day_hour" + TimeFormatter.DAY.get(ts)).setTimestamp(ts);
                r.putColumn(concat(Ints.toByteArray(hour), userid), position);

                r = mb.withRow(POSITIONS, "positions_hour_minutes" + hour).setTimestamp(ts);
                r.putColumn(concat(Ints.toByteArray(minutes), userid), position);

                // cell
                r = mb.withRow(POSITIONS, "cell1_" + hour).setTimestamp(ts);
                r.putColumn(concat(Ints.toByteArray(cell10), Ints.toByteArray(cell1), userid), position);

                r = mb.withRow(POSITIONS, "cell10_" + hour).setTimestamp(ts);
                r.putColumn(concat(Ints.toByteArray(cell10), userid), position);

            }
        }
    }

    public void process(MutationBatch mb, AisPacket packet) {
        AisMessage message = packet.tryGetAisMessage();
        long ts = packet.getTimestamp().getTime();

        messagesTime(mb, packet, message, ts);
        messagesMmsi(mb, packet, message, ts);
        messagesCell1(mb, packet, message, ts);
        messagesCell10(mb, packet, message, ts);
        positions(mb, packet, message, ts);
        // mmsi(mb, packet, message, ts);
    }

}
