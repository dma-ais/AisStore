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
package dk.dma.ais.store.materialize.write;

import static dk.dma.ais.store.AisStoreSchema.storeByArea;
import static dk.dma.ais.store.AisStoreSchema.storeByMmsi;
import static dk.dma.ais.store.AisStoreSchema.storeByTime;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.stream.Monitor;
import dk.dma.ais.store.write.CassandraBatchedStagedWriter;
import dk.dma.ais.store.write.DefaultAisStoreWriter;
import dk.dma.commons.tracker.PositionTracker;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.model.geometry.Position;

/**
 * A monitored writer, 
 * Based on the normal DefaultAisStoreWriter, which is used in the Archiver cli. 
 * 
 * @author Jens Tuxen
 */
public abstract class MonitoredAisStoreWriter extends DefaultAisStoreWriter {

    /**
     * The duration in milliseconds from when the latest positional message is received for a specific mmsi number is
     * still valid.
     */
    // TODO different for sat packets???
    public static final long POSITION_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

    /** A position tracker used to keeping an eye on previously received messages. */
    private final PositionTracker<Integer> tracker = new PositionTracker<>();
    
    protected Monitor monitor;
    protected Session viewSession;
    protected Cluster viewCluster;
    
    /**
     * @param connection
     * @param batchSize
     */
    public MonitoredAisStoreWriter(CassandraConnection connection, int batchSize, Cluster viewCluster, String viewKeySpace, boolean dummy, String monitorType, PrintWriter pw) {
        super(connection, batchSize);
        this.viewSession = viewCluster.connect(viewKeySpace);
        monitor = new Monitor(connection, viewCluster, viewSession, dummy, monitorType, pw);
        this.viewCluster = viewCluster;
    }

    public void handleMessage(List<RegularStatement> batch, AisPacket packet) {
        long ts = packet.getBestTimestamp();
        if (ts > 0) { // only save packets with a valid timestamp

            byte[] hash = Hashing.murmur3_128().hashUnencodedChars(packet.getStringMessage()).asBytes();

            byte[] column = Bytes.concat(Longs.toByteArray(ts), hash); // the column
            byte[] data = packet.toByteArray(); // the serialized packet

            storeByTime(batch, ts, column, data); // Store packet by time

            // packets are only stored by time, if they are not a proper message
            AisMessage message = packet.tryGetAisMessage();
            if (message == null) {
                return;
            }

            storeByMmsi(batch, ts, column, message.getUserId(), data); // Store packet by mmsi

            Position p = message.getValidPosition();
            if (p == null) { // Try to find an estimated position
                // Use the last received position message unless the position has timed out (POSITION_TIMEOUT_MS)
                p = tracker.getLatestIfLaterThan(message.getUserId(), ts - POSITION_TIMEOUT_MS);
            } else { // Update the tracker with latest position
                tracker.update(message.getUserId(), p.withTime(ts));
            }
            storeByArea(batch, ts, column, message.getUserId(), p, data);
            
            //fate share
            this.monitor.accept(packet);
        }
    }
}
