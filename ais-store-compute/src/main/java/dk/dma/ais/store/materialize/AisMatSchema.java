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
package dk.dma.ais.store.materialize;

import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;

import dk.dma.ais.store.AisStoreSchema;

/**
 * 
 * @author Jens Tuxen
 *
 */
public class AisMatSchema {
    public static final String KEYSPACE = AisStoreSchema.COLUMN_AISDATA;
    public static final String VIEW_KEYSPACE = "aismat";

    public static final String VIEW = "view_all";
    public static final String VALUE = "value";
    
    public static final String DAY_FORMAT = "yyyyMMdd";
    public static final String HOUR_FORMAT = "yyyyMMddHH";
    public static final String MONTH_FORMAT = "yyyyMM";
    public static final String MINUTE_FORMAT = "yyyyMMddHH:MM";
    
    public static final String CELL1_KEY = "cellid";
    public static final String CELL10_KEY = "cellid";  
    
    public static final String TABLE_MMSI_TIME_COUNT = "mmsi_time_count";
    public static final String TABLE_SOURCE_TIME_COUNT = "source_time_count";
    public static final String TABLE_CELL1_SOURCE_TIME_COUNT = "cell1_source_time_count";
    public static final String TABLE_CELL1_TIME_COUNT = "cell1_time_count";
    public static final String TABLE_STREAM_MONITOR = "stream_monitor";;
    
    
    public static final String MMSI_KEY = "mmsi";
    public static final String TIME_KEY = "time";
    public static final String SOURCE_KEY = "source";
    public static final String RESULT_KEY = "result";
    public static final String STREAM_TIME_KEY = "timeblock";;
    

    /**
     * Converts a milliseconds since epoch to a 10-minute blocks since epoch.
     * 
     * @param timestamp
     *            the timestamp to convert
     * @return the converted value
     */
    public static int getTimeBlock(long timestamp) {
        return Ints.checkedCast(timestamp / 10 / 60 / 1000);
    }
    
    public static int getTimeBlock(long timestamp, TimeUnit unit) {
        switch (unit) {
        case HOURS:
            return Ints.checkedCast(timestamp / 60 / 60 / 1000);
        case DAYS:
            return Ints.checkedCast(timestamp / 24 / 60 / 60 / 1000);            
        default:
            return getTimeBlock(timestamp);
        }
        
    }
    

}

