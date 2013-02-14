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
package dk.dma.ais.store.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.joda.time.Interval;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.util.CellPositionMmsi;
import dk.dma.ais.store.util.CellResolution;
import dk.dma.enav.model.geometry.Area;
import dk.dma.enav.model.geometry.PositionTime;

/**
 * 
 * @author Kasper Nielsen
 */
public interface MessageQueryService {

    Query<AisPacket> findByTime(Date start, Date end);

    Query<AisPacket> findByTime(String interval);

    Query<AisPacket> findByTime(Interval interval);

    Query<AisPacket> findByShape(Area shape, Date start, Date end);

    Query<AisPacket> findByShape(Area shape, long timeback, TimeUnit unit);

    /**
     * Finds all packets for the specified mmsi number in the given interval.
     * 
     * @param mmsi
     *            the mssi number
     * @param start
     *            the start date (inclusive)
     * @param end
     *            the end date (exclusive)
     * @return a query
     * @throws Exception
     *             something went wrong
     */
    Query<AisPacket> findByMMSI(Date start, Date end, int mmsi);

    Query<AisPacket> findByMMSI(String interval, int mmsi);

    Query<AisPacket> findByMMSI(Interval interval, int mmsi);

    /**
     * Finds the positions of all vessels at the specified date. If the specified time resolution is
     * {@link TimeUnit#HOURS} it will return the latest reported position from the same hour as the specified date.
     * Likewise if the specified time resolution is {@link TimeUnit#DAYS} it will return the latest reported position
     * from the same day as the specified date.
     * 
     * @param date
     *            the date
     * @param timeResolution
     *            the resolution either {@link TimeUnit#HOURS} or {@link TimeUnit#DAYS}
     * @return a result object
     * @throws IllegalArgumentException
     *             if time resolution is not either {@link TimeUnit#HOURS} or {@link TimeUnit#DAYS}
     * @throws Exception
     */
    Query<PositionTime> findAllPositions(Date date, TimeUnit timeResolution) throws Exception;

    Query<PositionTime> findCells(Date date, TimeUnit timeResolution) throws Exception;

    // uses mmsi_hour or mmsi_day
    Query<CellPositionMmsi> findCells(int mmsi, Date start, Date stop, CellResolution cellResolution) throws Exception;

    Query<CellPositionMmsi> findCells(int mmsi, long timeback, TimeUnit unit, CellResolution resolution)
            throws Exception;

    Query<CellPositionMmsi> findInCell(int cellId, CellResolution resolution, Date start, Date end) throws Exception;

    // uses
    Query<CellPositionMmsi> findInCell(int cellId, CellResolution resolution, long timeback, TimeUnit unit)
            throws Exception;

    Query<PositionTime> findPositions(int mmsi, Date date, CellResolution cellResolution, TimeUnit timeResolution)
            throws Exception;

    Query<PositionTime> findPositions(int mmsi, long timeback, TimeUnit unit, CellResolution resolution,
            TimeUnit timeResolution) throws Exception;
}
