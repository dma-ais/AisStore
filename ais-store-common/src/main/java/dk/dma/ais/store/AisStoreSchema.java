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

import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import dk.dma.ais.packet.AisPacket;

/**
 * This file contains the schema that is being used to store data in AisStore. It also contains various utility methods.
 * 
 * @author Kasper Nielsen
 */
public class AisStoreSchema {

    /** This table contains AIS packets ordered by timeblock and geographic cells of size 1 degree. */
    public static final String TABLE_AREA_CELL1 = "packets_area_cell1";

    /** This table contains AIS packets ordered by timeblock and geographic cells of size 10 degrees. */
    public static final String TABLE_AREA_CELL10 = "packets_area_cell10";

    /** This table holds AIS packets ordered by MMSI number with an unknown position. */
    public static final String TABLE_AREA_UNKNOWN = "packets_area_unknown";

    /**
     * This table holds all packets (with a valid message) stored in row with the MMSI number as the key. The columns
     * are ordered by timestamp and a message digest to avoid duplicates.
     */
    public static final String TABLE_MMSI = "packets_mmsi";

    /**
     * This table holds all packets stored in row with the number of 10 minute blocks since the
     * epoch as the key. Within each 10 minute blocks, packets are store ordered by timestamp (and a message
     * digest to avoid duplicates).
     */
    public static final String TABLE_TIME = "packets_time";

    // ---

    /** Common name of column holding time block (i.e. no. of 10 minute blocks since the epoch. */
    public static final String COLUMN_TIMEBLOCK = "timeblock";

    /** Common name of column holding timestamp with millisecond precision. */
    public static final String COLUMN_TIMESTAMP = "time";

    /** Common name of column holding cellid (geographical area) */
    public static final String COLUMN_CELLID = "cellid";

    /** Common name of column holding MMSI no. */
    public static final String COLUMN_MMSI = "mmsi";

    /** Common name of column holding aisdata message digest (to avoid storing duplicates) */
    public static final String COLUMN_AISDATA_DIGEST = "digest";

    /** We store the actual AIS message in this column. */
    public static final String COLUMN_AISDATA = "aisdata";

    /**
     * Converts a milliseconds since epoch to a 10-minute blocks since epoch.
     *
     * @param timestamp
     *            the timestamp to convert
     * @return the converted value
     */
    public static final int getTimeBlock(long timestamp) {
        return Ints.checkedCast(timestamp/10/60/1000);
    }

    /**
     * Calculates a message digest for the given messages (AisPackets).
     */
    public static final byte[] getDigest(AisPacket packet) {
        return Hashing.murmur3_128().hashUnencodedChars(packet.getStringMessage()).asBytes();
    }
}
