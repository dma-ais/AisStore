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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreSchema;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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

    final AtomicLong packetsProcessed = new AtomicLong(1);

    /**
     * A position tracker used to keeping an eye on previously received
     * messages.
     */
    private final Cache<Integer, PositionTime> tracker = CacheBuilder
            .newBuilder()
            .expireAfterWrite(POSITION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();

    private PacketsTimeSSTableWriter packetsTimeWriter;
    private PacketsMmsiSSTableWriter packetsMmsiWriter;
    private PacketsAreaCell1SSTableWriter packetsAreaCell1Writer;
    private PacketsAreaCell10SSTableWriter packetsAreaCell10Writer;
    private PacketsAreaUnknownSSTableWriter packetsAreaUnknownWriter;

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

        packetsTimeWriter = new PacketsTimeSSTableWriter(inDirectory, keyspace);
        packetsMmsiWriter = new PacketsMmsiSSTableWriter(inDirectory, keyspace);
        packetsAreaCell1Writer = new PacketsAreaCell1SSTableWriter(inDirectory, keyspace);
        packetsAreaCell10Writer = new PacketsAreaCell10SSTableWriter(inDirectory, keyspace);
        packetsAreaUnknownWriter = new PacketsAreaUnknownSSTableWriter(inDirectory, keyspace);
    }

    private void process(AisPacket packet) throws IOException {
        long ts = packet.getBestTimestamp();
        if (ts > 0) { // only save packets with a valid timestamp
            packetsTimeWriter.addPacket(packet);

            AisMessage message = packet.tryGetAisMessage();
            if (message != null) {
                packetsMmsiWriter.addPacket(packet);

                Position p = message.getValidPosition();

                if (p == null) {
                    // Try to find an estimated position
                    // Use the last received position message unless the position
                    // has timed out (POSITION_TIMEOUT_MS)
                    p = tracker.asMap().getOrDefault(message.getUserId(), null);
                } else {
                    // Update the tracker with latest position
                    // but only update the tracker IF the new time is better
                    tracker.asMap().merge(message.getUserId(), p.withTime(ts), (a, b) -> a.getTime() > b.getTime() ? a : b);
                }

                if (p != null && Position.isValid(p.getLatitude(), p.getLongitude())) {
                    packetsAreaCell1Writer.addPacket(packet, p);
                    packetsAreaCell10Writer.addPacket(packet, p);
                } else {
                    packetsAreaUnknownWriter.addPacket(packet);
                }
            }
        }

        packetsProcessed.incrementAndGet();
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

    public long numberOfPacketsProcessed() {
        return packetsProcessed.longValue();
    }
}
