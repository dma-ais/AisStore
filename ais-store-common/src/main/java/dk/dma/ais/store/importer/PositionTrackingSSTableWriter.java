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
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.model.geometry.PositionTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Simple SSTableGenerator
 * 
 * @author Jens Tuxen
 * @author Thomas Borg Salling
 */
public abstract class PositionTrackingSSTableWriter extends SSTableWriter {

    static final Logger LOG = LoggerFactory.getLogger(PositionTrackingSSTableWriter.class);

    public static final long POSITION_TIMEOUT_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

    /**
     * A position tracker used to keeping an eye on previously received
     * messages.
     */
    private final Cache<Integer, PositionTime> tracker = CacheBuilder
            .newBuilder()
            .expireAfterWrite(POSITION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();

    protected PositionTrackingSSTableWriter(String outputDir, String keyspace, String schemaDefinition, String insertStatement) {
        super(outputDir, keyspace, schemaDefinition, insertStatement);
    }

    protected Position targetPosition(AisPacket packet) {
        Position targetPosition = null;

        AisMessage message = packet.tryGetAisMessage();
        if (message != null) {
            targetPosition = message.getValidPosition();

            if (targetPosition == null) {
                // Try to find an estimated position
                // Use the last received position message unless the position
                // has timed out (POSITION_TIMEOUT_MS)
                targetPosition = tracker.asMap().getOrDefault(message.getUserId(), null);
            } else {
                final long ts = packet.getBestTimestamp();
                // Update the tracker with latest position
                // but only update the tracker IF the new time is better
                tracker.asMap().merge(message.getUserId(), targetPosition.withTime(ts), (a, b) -> a.getTime() > b.getTime() ? a : b);
            }
        }

        return targetPosition;
    }

    protected static boolean isValid(Position position) {
        return position != null && Position.isValid(position.getLatitude(), position.getLongitude());
    }

}
