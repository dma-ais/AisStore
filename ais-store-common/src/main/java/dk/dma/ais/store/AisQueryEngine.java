/*
 * Copyright (c) 2008 Kasper Nielsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.dma.ais.store;

import org.joda.time.Interval;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.cassandra.CassandraAisQueryEngine;
import dk.dma.db.Query;
import dk.dma.enav.model.geometry.Area;

/**
 * 
 * @author Kasper Nielsen
 * @see CassandraAisQueryEngine
 */
public interface AisQueryEngine {

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
     */
    Query<AisPacket> findByArea(Area shape, Interval interval);

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
     */
    Query<AisPacket> findByTime(Interval interval);
}

//
//
// /**
// * Finds the positions of all vessels at the specified date. If the specified time resolution is
// * {@link TimeUnit#HOURS} it will return the latest reported position from the same hour as the specified date.
// * Likewise if the specified time resolution is {@link TimeUnit#DAYS} it will return the latest reported position
// * from the same day as the specified date.
// *
// * @param date
// * the date
// * @param timeResolution
// * the resolution either {@link TimeUnit#HOURS} or {@link TimeUnit#DAYS}
// * @return a result object
// * @throws IllegalArgumentException
// * if time resolution is not either {@link TimeUnit#HOURS} or {@link TimeUnit#DAYS}
// * @throws Exception
// */
// Query<PositionTime> findAllPositions(Date date, TimeUnit timeResolution) throws Exception;
//
//
//
// Query<PositionTime> findCells(Date date, TimeUnit timeResolution) throws Exception;
//
// // uses mmsi_hour or mmsi_day
// Query<CellPositionMmsi> findCells(int mmsi, Interval interval, CellResolution cellResolution) throws Exception;
//
// Query<CellPositionMmsi> findInCell(int cellId, CellResolution resolution, Interval interval) throws Exception;
//
// Query<PositionTime> findPositions(int mmsi, Interval interval, CellResolution resolution, TimeUnit
// timeResolution)
// throws Exception;
