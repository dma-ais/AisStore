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
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.cassandra.exceptions.ConfigurationException;
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
    static final Logger LOG = LoggerFactory
            .getLogger(AisStoreSSTableGenerator.class);

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

    

    private AisStoreTableWriter packetsTimeWriter;

    private AisStoreTableWriter packetsMmsiWriter;

    private AisStoreTableWriter packetsAreaCell1Writer;

    private AisStoreTableWriter packetsAreaCell10Writer;

    private AisStoreTableWriter packetsAreaUnknownWriter;

    public AisStoreSSTableGenerator(String inDirectory, String keyspace, String compressor, int bufferSize) throws ConfigurationException, IOException, URISyntaxException {
        Arrays.asList(AisStoreSchema.TABLE_MMSI, AisStoreSchema.TABLE_TIME,
                AisStoreSchema.TABLE_AREA_CELL1,
                AisStoreSchema.TABLE_AREA_CELL10,
                AisStoreSchema.TABLE_AREA_UNKNOWN).stream().sequential()
                .forEach(directory -> {
                    try {
                        Files.createDirectories(Paths.get(inDirectory,keyspace, directory));
                    } catch (FileAlreadyExistsException e) {
                        //do nothing
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                });

        packetsTimeWriter = AisStoreTableWriters.newPacketTimeWriter(
                inDirectory, keyspace, compressor, bufferSize);
        packetsMmsiWriter = AisStoreTableWriters.newPacketMmsiWriter(
                inDirectory, keyspace, compressor, bufferSize);
        packetsAreaCell1Writer = AisStoreTableWriters.newPacketAreaCell1Writer(
                inDirectory, keyspace, compressor, bufferSize);
        packetsAreaCell10Writer = AisStoreTableWriters
                .newPacketAreaCell10Writer(inDirectory, keyspace, compressor, bufferSize);
        packetsAreaUnknownWriter = AisStoreTableWriters
                .newPacketAreaUnknownWriter(inDirectory, keyspace, compressor, bufferSize);
        packetsTimeWriter = AisStoreTableWriters.newPacketTimeWriter(
                inDirectory, keyspace, compressor, bufferSize);
    }

    private void process(AisPacket packet) throws IOException {

        long ts = packet.getBestTimestamp();
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

            packetsTimeWriter.addRow(new ByteBuffer[] { timeblock, timehash,
                    aisdata }, ts);

            // packets are only stored by time, if they are not a proper message
            AisMessage message = packet.tryGetAisMessage();
            if (message == null) {
                return;
            }

            ByteBuffer mmsi = ByteBuffer.wrap(Ints.toByteArray(message
                    .getUserId()));

            packetsMmsiWriter.addRow(
                    new ByteBuffer[] { mmsi, timehash, aisdata }, ts);

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



            if (p != null
                    && Position.isValid(p.getLatitude(), p.getLongitude())) {
                
                ByteBuffer cell1 = ByteBuffer.wrap(Ints.toByteArray(p
                        .getCellInt(1.0)));
                ByteBuffer cell10 = ByteBuffer.wrap(Ints.toByteArray(p
                        .getCellInt(10.0)));
                
                packetsAreaCell1Writer.addRow(new ByteBuffer[] { cell1,
                        timehash, aisdata }, ts);
                packetsAreaCell10Writer.addRow(new ByteBuffer[] { cell10,
                        timehash, aisdata }, ts);
            } else {
                packetsAreaUnknownWriter.addRow(new ByteBuffer[] { mmsi,
                        timehash, aisdata }, ts);
            }

        }

    }

    /**
     * 
     * @param inDirectory
     *            which directory to place sstables in
     * @return
     * @throws ConfigurationException 
     * @throws URISyntaxException 
     * @throws IOException 
     */
    public static AisStoreSSTableGenerator createAisStoreSSTableGenerator(
            String inDirectory, String keyspace, String compressor, int bufferSize) throws ConfigurationException, IOException, URISyntaxException {

        return new AisStoreSSTableGenerator(inDirectory, keyspace, compressor, bufferSize);

    }

    public void close() throws IOException {
        packetsTimeWriter.close();
        packetsMmsiWriter.close();
        packetsAreaCell10Writer.close();
        packetsAreaUnknownWriter.close();
        packetsAreaCell1Writer.close();
    }

    @Override
    public void accept(AisPacket t) {
        try {
            this.process(t);
        } catch (IOException e) {
            LOG.warn("Failed to convert packet with timestamp "
                    + t.getBestTimestamp());
            e.printStackTrace();
        }

    }

}
