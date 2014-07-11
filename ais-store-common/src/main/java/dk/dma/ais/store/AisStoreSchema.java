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
package dk.dma.ais.store;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.primitives.Ints;

import dk.dma.enav.model.geometry.Position;

/**
 * This file contains the schema that is being used to store data in AisStore. It also contains various utility methods.
 * 
 * @author Kasper Nielsen
 */
public class AisStoreSchema {

    /** We store the actual AIS message in this column. */
    public static final String COLUMN_AISDATA = "aisdata";

    /**
     * The concatenation of timestamp and has of the packet, that is used together with the column key as the combined
     * key.
     */
    public static final String COLUMN_TIMEHASH = "timehash";

    /** This table contains all messages received for each 1 degree cell. */
    public static final String TABLE_AREA_CELL1 = "packets_area_cell1";

    /** The key column name of {@link #TABLE_AREA_CELL1_KEY}. */
    public static final String TABLE_AREA_CELL1_KEY = "cellid";

    /** This table contains all messages received for each 10 degree cell. */
    public static final String TABLE_AREA_CELL10 = "packets_area_cell10";

    /** The key column name of {@link #TABLE_AREA_CELL10_KEY}. */
    public static final String TABLE_AREA_CELL10_KEY = "cellid";

    /** This table holds all packets (with a valid message) ordered by MMSI number with an unknown position. */
    public static final String TABLE_AREA_UNKNOWN = "packets_area_unknown";

    /** The key column name of {@link #TABLE_AREA_UNKNOWN}. */
    public static final String TABLE_AREA_UNKNOWN_KEY = "mmsi";

    /**
     * This table holds all packets (with a valid message) stored in row with the MMSI number as the key. The columns
     * are ordered by timestamp concatenated with a hash of the packet being stored.
     */
    public static final String TABLE_MMSI = "packets_mmsi";

    /** The key column name of {@link #TABLE_MMSI_KEY}. */
    public static final String TABLE_MMSI_KEY = "mmsi";

    /**
     * This table holds all packets (with a valid message) stored in row with the number of 10 minute blocks since the
     * epoch as the key. Within each 10 minute blocks, packets are store ordered by timestamp concatenated with a hash
     * of the message.
     */
    public static final String TABLE_TIME = "packets_time";

    /** The key column name of {@link #TABLE_TIME_KEY}. */
    public static final String TABLE_TIME_KEY = "timeblock";

    /**
     * Converts a milliseconds since epoch to a 10-minute blocks since epoch.
     * 
     * @param timestamp
     *            the timestamp to convert
     * @return the converted value
     */
    static int getTimeBlock(long timestamp) {
        return Ints.checkedCast(timestamp / 10 / 60 / 1000);
    }

    /**
     * @param tableName
     *            the name of the table
     * @param keyName
     *            the name of the first part of the key
     * @param key
     *            the value of the first part of the key
     * @param time_hash
     *            the timestamp - AIS data hash value
     * @param aisData
     *            the AIS data to insert
     * @param timestamp
     *            the timestamp of the AIS packet
     * @return the insert statement
     */
    private static Insert store(String tableName, String keyName, int key, byte[] time_hash, byte[] aisData,
            long timestamp) {
        Insert i = QueryBuilder.insertInto(tableName);
        i.value(keyName, key);
        i.value(COLUMN_TIMEHASH, ByteBuffer.wrap(time_hash));
        i.value(COLUMN_AISDATA, ByteBuffer.wrap(aisData));
        i.using(QueryBuilder.timestamp(timestamp));
        return i;
    }

    /** Stores the specified packet by position (area). */
    public static void storeByArea(List<RegularStatement> batch, long timestamp, byte[] column, int mmsi, Position p,
            byte[] packet) {
        if (p == null) {
            // Okay we have no idea of the position of the ship. Store it in this table and process it later
            batch.add(store(TABLE_AREA_UNKNOWN, TABLE_AREA_UNKNOWN_KEY, mmsi, column, packet, timestamp));
        } else {
            // Cells with size 1 degree
            batch.add(store(TABLE_AREA_CELL1, TABLE_AREA_CELL1_KEY, p.getCellInt(1), column, packet, timestamp));

            // Cells with size 10 degree
            batch.add(store(TABLE_AREA_CELL10, TABLE_AREA_CELL10_KEY, p.getCellInt(10), column, packet, timestamp));
        }
    }

    /** Stores the specified packet by MMSI. */
    public static void storeByMmsi(List<RegularStatement> batch, long timestamp, byte[] column, int mmsi, byte[] packet) {
        batch.add(store(TABLE_MMSI, TABLE_MMSI_KEY, mmsi, column, packet, timestamp));
    }

    /** Stores the specified packet by time. */
    public static void storeByTime(List<RegularStatement> batch, long timestamp, byte[] column, byte[] packet) {
        batch.add(store(TABLE_TIME, TABLE_TIME_KEY, getTimeBlock(timestamp), column, packet, timestamp));
    }
}
