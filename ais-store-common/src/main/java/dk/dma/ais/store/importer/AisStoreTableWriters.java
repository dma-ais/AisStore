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
package dk.dma.ais.store.importer;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.exceptions.ConfigurationException;

import dk.dma.ais.store.AisStoreSchema;
/**
 * Factory methods for schema/table writers
 * @author Jens Tuxen
 *
 */
public class AisStoreTableWriters {
    
    static final AisStoreTableWriter newPacketTimeWriter(String inDirectory,String keyspace) throws ConfigurationException {
        @SuppressWarnings("rawtypes")
        AbstractType[] types = {IntegerType.instance, BytesType.instance, BytesType.instance};
        List<String> keys = Arrays.asList(AisStoreSchema.TABLE_TIME_KEY,AisStoreSchema.COLUMN_TIMEHASH,AisStoreSchema.COLUMN_AISDATA);
        return new AisStoreTableWriter(inDirectory,keyspace,AisStoreSchema.TABLE_TIME, keys, 1024, types);
    }
    
    static final AisStoreTableWriter newPacketMmsiWriter(String inDirectory,String keyspace) throws ConfigurationException {
        @SuppressWarnings("rawtypes")
        AbstractType[] types = {IntegerType.instance, BytesType.instance, BytesType.instance};
        List<String> keys = Arrays.asList(AisStoreSchema.TABLE_MMSI_KEY,AisStoreSchema.COLUMN_TIMEHASH,AisStoreSchema.COLUMN_AISDATA);
        return new AisStoreTableWriter(inDirectory,keyspace,AisStoreSchema.TABLE_MMSI, keys, 256, types);
    }
    
    
    static final AisStoreTableWriter newPacketAreaCell1Writer(String inDirectory,String keyspace) throws ConfigurationException {
        @SuppressWarnings("rawtypes")
        AbstractType[] types = {IntegerType.instance, BytesType.instance, BytesType.instance};
        List<String> keys = Arrays.asList(AisStoreSchema.TABLE_AREA_CELL1_KEY,AisStoreSchema.COLUMN_TIMEHASH,AisStoreSchema.COLUMN_AISDATA);
        return new AisStoreTableWriter(inDirectory,keyspace,AisStoreSchema.TABLE_AREA_CELL1, keys, 1024, types);
    }
    
    static final AisStoreTableWriter newPacketAreaCell10Writer(String inDirectory,String keyspace) throws ConfigurationException {
        @SuppressWarnings("rawtypes")
        AbstractType[] types = {IntegerType.instance, BytesType.instance, BytesType.instance};
        List<String> keys = Arrays.asList(AisStoreSchema.TABLE_AREA_CELL10_KEY,AisStoreSchema.COLUMN_TIMEHASH,AisStoreSchema.COLUMN_AISDATA);
        return new AisStoreTableWriter(inDirectory,keyspace,AisStoreSchema.TABLE_AREA_CELL10, keys, 1024, types);
    }

    static final AisStoreTableWriter newPacketAreaUnknownWriter(String inDirectory,String keyspace) throws ConfigurationException {
        @SuppressWarnings("rawtypes")
        AbstractType[] types = {IntegerType.instance, BytesType.instance, BytesType.instance};
        List<String> keys = Arrays.asList(AisStoreSchema.TABLE_AREA_UNKNOWN_KEY,AisStoreSchema.COLUMN_TIMEHASH,AisStoreSchema.COLUMN_AISDATA);
        return new AisStoreTableWriter(inDirectory,keyspace,AisStoreSchema.TABLE_AREA_UNKNOWN, keys, 256, types);
    }
            

        
    

}
