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

import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.AbstractService;

import dk.dma.enav.model.geometry.Area;

/**
 * A connection to AisStore.
 * 
 * @author Kasper Nielsen
 */
public final class AisStoreConnection extends AbstractService {

    /** The cluster we are connected to. */
    private final Cluster cluster;

    /** The name of the keyspace. */
    private final String keyspace;

    /** The current session */
    private volatile Session session;

    private AisStoreConnection(Cluster cluster, String keyspace) {
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
     * Finds all packets recieved from within the specified area in the given interval.
     * 
     * @param area
     *            the area
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @return an iterable with all packets
     * @throws NullPointerException
     *             if the specified area is null
     */
    public AisStoreQueryResult findForArea(Area area, long startInclusive, long stopExclusive) {
        return AisStoreQuery.forArea(getSession(), area, startInclusive, stopExclusive);
    }

    /**
     * Finds all packets received in the given interval. Should only be used for small time intervals.
     * 
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @return an iterable with all packets
     */
    public AisStoreQueryResult findForTime(long startInclusive, long stopExclusive) {
        return AisStoreQuery.forTime(getSession(), startInclusive, stopExclusive);
    }

    /**
     * Finds all packets for one or more MMSI numbers in the given interval.
     * 
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @param mmsi
     *            one or more MMSI numbers
     * @return an iterable with all packets
     */
    public AisStoreQueryResult findForMmsi(long startInclusive, long stopExclusive, int... mmsi) {
        return AisStoreQuery.forMmsi(getSession(), startInclusive, stopExclusive, mmsi);
    }

    /** {@inheritDoc} */
    @Override
    protected void doStop() {
        cluster.shutdown();
        notifyStopped();
    }

    /**
     * Returns the current session.
     * 
     * @return the current session
     * @throws IllegalStateException
     *             if the connection has not yet been started via {@link #start()}
     */
    public Session getSession() {
        Session session = this.session;
        if (session == null) {
            throw new IllegalStateException("The connection has not been started via connection.start() yet");
        }
        return session;
    }

    /**
     * Creates a new AisStore connection. The connection is not yet connected but must be started via {@link #start()}
     * and to close it use {@link #stop()}
     * 
     * @param keyspace
     *            the name of the keyspace
     * @param connectionPoints
     *            the connection points
     * @return a new connection
     */
    public static AisStoreConnection create(String keyspace, String... connectionPoints) {
        return create(keyspace, Arrays.asList(connectionPoints));
    }

    /**
     * Creates a new AisStore connection. The connection is not yet connected but must be started via {@link #start()}
     * and to close it use {@link #stop()}
     * 
     * @param keyspace
     *            the name of the keyspace
     * @param connectionPoints
     *            the connection points
     * @return a new connection
     */
    public static AisStoreConnection create(String keyspace, List<String> connectionPoints) {
        Cluster cluster = Cluster.builder().addContactPoints(connectionPoints.toArray(new String[0])).build();
        return new AisStoreConnection(cluster, keyspace);
    }
}
