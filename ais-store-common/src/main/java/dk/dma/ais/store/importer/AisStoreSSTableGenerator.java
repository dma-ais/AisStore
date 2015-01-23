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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.CQLSSTableWriter.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreSchema;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;

/**
 * Simple SSTableGenerator
 * 
 * @author Jens Tuxen
 *
 */
public class AisStoreSSTableGenerator implements Consumer<AisPacket> {
    
    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(AisStoreSSTableGenerator.class);

    private static final String packetsTimeSchema = "CREATE TABLE aisdata.packets_time (timeblock int,timehash blob,aisdata blob,PRIMARY KEY (timeblock, timehash))";//+"WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND comment = 'Contains AIS data ordered by receive time.'";
    private static final String packetsMmsiSchema = "CREATE TABLE aisdata.packets_mmsi (mmsi int,timehash blob,aisdata blob,PRIMARY KEY (mmsi, timehash)) WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':256} AND comment     = 'Contains AIS data ordered by mmsi number.'";
    private static final String packetsAreaCell1Schema = "CREATE TABLE aisdata.packets_area_cell1 (cellid int,timehash blob,aisdata blob,PRIMARY KEY (cellid, timehash)) WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND comment = 'Contains AIS data ordered by cells of size 1 degree.'";
    private static final String packetsAreaCell10Schema = "CREATE TABLE aisdata.packets_area_cell10 (cellid int,timehash blob,aisdata blob,PRIMARY KEY (cellid, timehash)) WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND comment = 'Contains AIS data ordered by cells of size 10 degree.'";
    private static final String packetsAreaUnknownSchema = "CREATE TABLE aisdata.packets_area_unknown (mmsi int,timehash blob,aisdata blob,PRIMARY KEY (mmsi, timehash)) WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':256} AND comment = 'Contains AIS data where the area has not yet been determined.'";

    CQLSSTableWriter packetsTime = null;
    CQLSSTableWriter packetsMmsi = null;
    CQLSSTableWriter packets_area_cell1 = null;
    CQLSSTableWriter packets_area_cell10 = null;
    CQLSSTableWriter packets_area_unknown = null;

    public static final long POSITION_TIMEOUT_MS = TimeUnit.MILLISECONDS
            .convert(20, TimeUnit.MINUTES);

    /**
     * A position tracker used to keeping an eye on previously received
     * messages.
     */
    private final Cache<Integer, PositionTime> tracker = CacheBuilder
            .newBuilder()
            .expireAfterWrite(POSITION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();

    public AisStoreSSTableGenerator(String inDirectory) {
        Builder builder = CQLSSTableWriter
                .builder();

        
        packetsTime = builder
                .inDirectory(inDirectory)
                .forTable(packetsTimeSchema)
                .using("INSERT INTO aisdata.packets_time (timeblock, timehash, aisdata) VALUES (?, ?, ?) using TIMESTAMP ?")
                .withPartitioner(new Murmur3Partitioner())
                .withBufferSizeInMB(64)
                .build();
        
        
        /*
        packetsMmsi = builder
                .inDirectory(inDirectory+"packets_mmsi")
                .forTable(packetsMmsiSchema)
                .using("INSERT INTO aisdata.packets_mmsi (mmsi, timehash, aisdata) VALUES (?,?,?) using TIMESTAMP ?")
                .withPartitioner(new Murmur3Partitioner())
                .build();

        packets_area_cell1 = builder
                .inDirectory(inDirectory)
                .forTable(packetsAreaCell1Schema)
                .using("INSERT INTO aisdata.packets_area_cell1 (cellid, timehash, aisdata) VALUES (?,?,?) using TIMESTAMP ?")
                .withPartitioner(new Murmur3Partitioner())
                .build();

        packets_area_cell10 = builder
                .inDirectory(inDirectory)
                .forTable(packetsAreaCell10Schema)
                .using("INSERT INTO aisdata.packets_area_cell10 (cellid, timehash, aisdata) VALUES (?,?,?) using TIMESTAMP ?")
                .withPartitioner(new Murmur3Partitioner())
                .build();

        packets_area_unknown = builder
                .inDirectory(inDirectory+"_packets_area_unknown")
                .forTable(packetsAreaUnknownSchema)
                .using("INSERT INTO aisdata.packets_area_unknown (mmsi, timehash, aisdata) VALUES (?,?,?) using TIMESTAMP ?")
                .withPartitioner(new Murmur3Partitioner())
                .build();
                
                */

    }

    private void process(AisPacket packet) throws InvalidRequestException, IOException {

        long ts = packet.getBestTimestamp();
        ByteBuffer tsByteBuffer = ByteBuffer.wrap(Longs.toByteArray(ts));
        if (ts > 0) { // only save packets with a valid timestamp

            byte[] hash = Hashing.murmur3_128()
                    .hashUnencodedChars(packet.getStringMessage()).asBytes();
            byte[] column = Bytes.concat(Longs.toByteArray(ts), hash); // the
                                                                       // column
            byte[] data = packet.toByteArray(); // the serialized packet

            ByteBuffer timeblock = ByteBuffer.wrap(Ints
                    .toByteArray((AisStoreSchema.getTimeBlock(ts))));
            ByteBuffer timehash = ByteBuffer.wrap(column);
            ByteBuffer aisdata = ByteBuffer.wrap(data);

            
            packetsTime.rawAddRow(timeblock, timehash, aisdata, tsByteBuffer);

            /*
            // packets are only stored by time, if they are not a proper message
            AisMessage message = packet.tryGetAisMessage();
            if (message == null) {
                return;
            }

            ByteBuffer mmsi = ByteBuffer.wrap(Ints.toByteArray(message
                    .getUserId()));
            //packetsMmsi.rawAddRow(mmsi, timehash, aisdata, tsByteBuffer);

            Position p = message.getValidPosition();

            if (p == null) { // Try to find an estimated position
                // Use the last received position message unless the position
                // has timed out (POSITION_TIMEOUT_MS)
                p = tracker.asMap().getOrDefault(message.getUserId(), null);
            } else { // Update the tracker with latest position

                // but only update the tracker IF the new time is better
                tracker.asMap().merge(message.getUserId(), p.withTime(ts),
                        (a, b) -> {
                            return a.getTime() > b.getTime() ? a : b;
                        });
            }

            
            ByteBuffer cell1 = ByteBuffer.wrap(Ints.toByteArray(p
                    .getCellInt(1.0)));
            ByteBuffer cell10 = ByteBuffer.wrap(Ints.toByteArray(p
                    .getCellInt(10.0)));
            
            if (p != null && Position.isValid(p.getLatitude(), p.getLongitude())) {
                //packets_area_cell1.rawAddRow(cell1, timehash, aisdata,
                //        tsByteBuffer);
                //packets_area_cell10.rawAddRow(cell10, timehash, aisdata,
                //        tsByteBuffer);
            } else {
                //packets_area_unknown.rawAddRow(mmsi, timehash, aisdata,
                //        tsByteBuffer);
            }
            */
            
        }
            
       
    }

    /**
     * 
     * @param inDirectory which directory to place sstables in
     * @return
     */
    public static AisStoreSSTableGenerator createAisStoreSSTableGenerator(
            String inDirectory) {

        return new AisStoreSSTableGenerator(inDirectory);

    }
    
    
    public void stop() throws IOException {
        packetsTime.close();
    }

    @Override
    public void accept(AisPacket t) {
        try {
            this.process(t);
        } catch (InvalidRequestException | IOException e) {
            LOG.warn("Failed to convert packet");
            e.printStackTrace();
        }
        
    }

}
