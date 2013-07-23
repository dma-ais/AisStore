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

import org.joda.time.Interval;

import dk.dma.ais.packet.AisPacket;
import dk.dma.enav.model.geometry.Area;

/**
 * The entry interface to query AisStore.
 * 
 * @author Kasper Nielsen
 */
public interface AisStore {

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
    Query<AisPacket> findByArea(Area area, Interval interval);

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
    Query<AisPacket> findByMMSI(int mmsi, Interval interval);

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
    Query<AisPacket> findByTime(Interval interval);
}
