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
package dk.dma.ais.store;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.AbstractService;

import dk.dma.ais.packet.AisPacket;
import dk.dma.enav.model.geometry.Area;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisStoreConnection extends AbstractService {
    private final Cluster cluster;
    private final String keyspace;

    private volatile Session session;

    AisStoreConnection(Cluster cluster, String keyspace) {
        this.cluster = requireNonNull(cluster);
        this.keyspace = requireNonNull(keyspace);
    }

    /** {@inheritDoc} */
    @Override
    protected void doStart() {
        session = cluster.connect(keyspace);
        notifyStarted();
    }

    /**
     * Finds all packets for the specified area in the given interval.
     * 
     * @param area
     *            the area
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @return a new query
     * @throws NullPointerException
     *             if the specified area or interval is null
     */
    public Iterable<AisPacket> queryForArea(Area area, long startInclusive, long stopExclusive) {
        return AisStoreQueries.forArea(session, area, startInclusive, stopExclusive);
    }

    /**
     * Finds all packets received in the given interval. Should only be used for small time intervals.
     * 
     * @param mmsi
     *            the mssi number
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @return a new query
     * @throws NullPointerException
     *             if the specified interval is null
     */
    public Iterable<AisPacket> queryForTime(long startInclusive, long stopExclusive) {
        return AisStoreQueries.forTime(session, startInclusive, stopExclusive);
    }

    /**
     * Finds all packets for the specified mmsi number in the given interval.
     * 
     * @param mmsi
     *            the mssi number
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @return a new query
     * @throws NullPointerException
     *             if the specified interval is null
     */
    public Iterable<AisPacket> queryForMmsi(int mmsi, long startInclusive, long stopExclusive) {
        return AisStoreQueries.forMmsi(session, mmsi, startInclusive, stopExclusive);
    }

    /** {@inheritDoc} */
    @Override
    protected void doStop() {
        cluster.shutdown();
        notifyStopped();
    }

    public Session getSession() {
        return session;
    }

    public static AisStoreConnection create(String keyspace, List<String> connectionPoints) {
        Cluster cluster = Cluster.builder().addContactPoints(connectionPoints.toArray(new String[0])).build();
        return new AisStoreConnection(cluster, keyspace);
    }
}
