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
package dk.dma.ais.store.cassandra.schema;

import org.apache.cassandra.utils.Hex;

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
import dk.dma.app.cassandra.CassandraWriteSink;
import dk.dma.app.util.TimeUtil;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
public class OldFullSchema extends CassandraWriteSink<AisPacket> {

    public static final OldFullSchema INSTANCE = new OldFullSchema();

    public final static ColumnFamily<byte[], String> MESSAGES = new ColumnFamily<>("aismessages",
            BytesArraySerializer.get(), StringSerializer.get());

    public final static ColumnFamily<String, byte[]> POSITIONS = new ColumnFamily<>("positions_tmp",
            StringSerializer.get(), BytesArraySerializer.get());

    public final static ColumnFamily<String, byte[]> POSITIONS_DETAIL = new ColumnFamily<>("positions_detail",
            StringSerializer.get(), BytesArraySerializer.get());

    public final static ColumnFamily<Integer, String> MMSI = new ColumnFamily<>("mmsi", IntegerSerializer.get(),
            StringSerializer.get());

    public static final String MESSAGES_MESSAGE = "m";
    public static final String MESSAGES_MMSI_HOUR = "h";

    public static final String MESSAGES_CELL1_MINUTE = "cell1_minute";
    public static final String MESSAGES_CELL1_HOUR = "cell1_hour";

    private void messages(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        ColumnListMutation<String> r = mb.withRow(MESSAGES,
                Bytes.concat(Longs.toByteArray(ts), packet.calculateHash128())).setTimestamp(ts);
        r.putColumn(MESSAGES_MESSAGE, packet.getStringMessage());
        if (message != null) {
            r.putColumn(MESSAGES_MMSI_HOUR, merge(message.getUserId(), TimeUtil.hoursSinceEpoch(ts)));
        }
    }

    private void cellOverviewDetail(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message instanceof IPositionMessage) {
            IPositionMessage m = (IPositionMessage) message;
            Position p = m.getPos().tryGetGeoLocation();
            if (p != null) {
                byte[] userid = Ints.toByteArray(message.getUserId());
                int cell10 = (int) p.getCell(10); // around 648 total cells
                int cell1 = (int) p.getCell(1); // around 64800 total cells
                int minutes = TimeUtil.minutesSinceEpoch(ts);
                int hour = TimeUtil.hoursSinceEpoch(ts);
                int days = TimeUtil.daysSinceEpoch(ts);

                long packetLong = p.toPackedLong();
                // cellresolution_hour
                // ColumnListMutation<byte[]> r = mb.withRow(POSITIONS, "cell1_" + hour).setTimestamp(ts);
                // r.putColumn(Bytes.concat(Ints.toByteArray(cell10), Ints.toByteArray(cell1), userid), packetLong);
                //
                // r = mb.withRow(POSITIONS, "cell10_" + hour).setTimestamp(ts);
                // r.putColumn(Bytes.concat(Ints.toByteArray(cell10), userid), packetLong);
                //
                // // mmsi_cellresolution
                // r = mb.withRow(POSITIONS, message.getUserId() + "_cell1").setTimestamp(ts);
                // r.putColumn(Bytes.concat(Ints.toByteArray(hour), Ints.toByteArray(cell1)), packetLong);
                //
                // r = mb.withRow(POSITIONS, message.getUserId() + "_cell10").setTimestamp(ts);
                // r.putColumn(Bytes.concat(Ints.toByteArray(hour), Ints.toByteArray(cell10)), packetLong);
                //
                // // mmsi_time (har i modsaetning til mmsi_cellresolution ikke alle celler den har vaeret i)
                // r = mb.withRow(POSITIONS, message.getUserId() + "_hour").setTimestamp(ts);
                // r.putColumn(Ints.toByteArray(hour), packetLong);
                //
                // r = mb.withRow(POSITIONS, message.getUserId() + "_day").setTimestamp(ts);
                // r.putColumn(Ints.toByteArray(days), packetLong);

                // All positions
            }
        }
    }

    private void cellOverview(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message instanceof IPositionMessage) {
            IPositionMessage m = (IPositionMessage) message;
            Position p = m.getPos().tryGetGeoLocation();
            if (p != null) {
                byte[] userid = Ints.toByteArray(message.getUserId());
                int cell10 = (int) p.getCell(10); // around 648 total cells
                int cell1 = (int) p.getCell(1); // around 64800 total cells
                int hour = TimeUtil.hoursSinceEpoch(ts);
                int days = TimeUtil.daysSinceEpoch(ts);
                int minutes = TimeUtil.minutesSinceEpoch(ts);

                long packetLong = p.toPackedLong();
                // cellresolution_hour
                ColumnListMutation<byte[]> r = mb.withRow(POSITIONS, "cell1_" + hour).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(cell10), Ints.toByteArray(cell1), userid), packetLong);

                r = mb.withRow(POSITIONS, "cell10_" + hour).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(cell10), userid), packetLong);

                // mmsi_cellresolution
                r = mb.withRow(POSITIONS, message.getUserId() + "_cell1").setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(hour), Ints.toByteArray(cell1)), packetLong);

                r = mb.withRow(POSITIONS, message.getUserId() + "_cell10").setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(hour), Ints.toByteArray(cell10)), packetLong);

                // mmsi_time (har i modsaetning til mmsi_cellresolution ikke alle celler den har vaeret i)
                r = mb.withRow(POSITIONS, message.getUserId() + "_hour").setTimestamp(ts);
                r.putColumn(Ints.toByteArray(hour), packetLong);

                r = mb.withRow(POSITIONS, message.getUserId() + "_day").setTimestamp(ts);
                r.putColumn(Ints.toByteArray(days), packetLong);

                // All positions
                r = mb.withRow(POSITIONS, "positions_" + hour).setTimestamp(ts);
                r.putColumn(userid, packetLong);

                r = mb.withRow(POSITIONS, "positions_" + days).setTimestamp(ts);
                r.putColumn(userid, packetLong);

                r = mb.withRow(POSITIONS, "positions_" + hour).setTimestamp(ts);
                r.putColumn(Bytes.concat(Ints.toByteArray(minutes), userid), packetLong);
            }
        }
    }

    void mmsi(MutationBatch mb, AisPacket packet, AisMessage message, long ts) {
        if (message != null) {
            ColumnListMutation<String> r = mb.withRow(MMSI, message.getUserId()).setTimestamp(ts);
            r.putColumn("last_message", packet.getStringMessage());
            r.putColumn("last_message_timestamp", ts);
            if (message instanceof IPositionMessage) {
                IPositionMessage m = (IPositionMessage) message;
                Position p = m.getPos().tryGetGeoLocation();
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

    public static void maidn(String[] args) {
        System.out.println(Hex.bytesToHex(Longs.toByteArray(1231312144423L)));
        System.out.println(Hex.bytesToHex(Longs.toByteArray(Long.rotateLeft(1231312144423L, 32))));
        // System.out.println();
    }

    public void process(MutationBatch mb, AisPacket packet) {
        AisMessage message = packet.tryGetAisMessage();
        long ts = packet.getTimestamp().getTime();

        // messages(mb, packet, message, ts);
        // mmsi(mb, packet, message, ts);

        cellOverview(mb, packet, message, ts);

        cellOverviewDetail(mb, packet, message, ts);
    }

    public static long merge(int a, int b) {
        return ((long) a << 32) + b;
    }

    public static long hash(long a, long b) {
        // we need to spread them out good, mmsi numbers and timestamp are usually not spread out
        return Long.rotateLeft(0x5DEECE66DL, 32) ^ spreadHash(b);
    }

    /** Applies a supplemental hash function to a given hashCode */
    static long spreadHash(long key) {
        key = ~key + (key << 21); // key = (key << 21) - key - 1;
        key = key ^ key >> 24;
        key = key + (key << 3) + (key << 8); // key * 265
        key = key ^ key >> 14;
        key = key + (key << 2) + (key << 4); // key * 21
        key = key ^ key >> 28;
        key = key + (key << 31);
        return key;
    }
}
