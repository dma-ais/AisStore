/* Copyright (c) 2011 Danish Maritime Authority.
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

import com.datastax.driver.core.Session;
import com.google.common.collect.AbstractIterator;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreSchema.Table;
import dk.dma.db.cassandra.CassandraQueryBuilder;
import dk.dma.enav.model.geometry.Area;
import dk.dma.enav.model.geometry.grid.Cell;
import dk.dma.enav.model.geometry.grid.Grid;
import org.joda.time.Interval;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;

import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_CELLID;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_MMSI;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMEBLOCK;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_AREA_CELL1;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_AREA_CELL10;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_MMSI;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_TIME;
import static java.util.Objects.requireNonNull;

/**
 * 
 * @author Kasper Nielsen
 */
public final class AisStoreQueryBuilder extends CassandraQueryBuilder<AisStoreQueryResult> {

    /** The bounding area. */
    final Area area;

    /** The number of results to fetch at a time. */
    int batchLimit = 3000; // magic constant found by trial;

    /** The list of MMSI number to retrieve. */
    final int[] mmsi;

    /** The start epoch time (inclusive) */
    Instant startTimeInclusive;

    /** The start epoch time (exclusive) */
    Instant stopTimeExclusive;

    private AisStoreQueryBuilder(Area area, int[] mmsi) {
        this.area = area;
        this.mmsi = mmsi;
    }

    protected AisStoreQueryResult execute(Session s) {
        requireNonNull(s);
        AisStoreQueryInnerContext inner = new AisStoreQueryInnerContext();
        ArrayList<AbstractIterator<AisPacket>> queries = new ArrayList<>();
        if (area != null) {
            Set<Cell> cells1 = Grid.GRID_1_DEGREE.getCells(area);
            Set<Cell> cells10 = Grid.GRID_10_DEGREES.getCells(area);

            int factor = 10;// magic constant

            // Determines if use the tables of size 1 degree, or size 10 degrees
            boolean useCell1 = cells10.size() * factor > cells1.size();
            Table table = useCell1 ? TABLE_PACKETS_AREA_CELL1 : TABLE_PACKETS_AREA_CELL10;
            Set<Cell> cells = useCell1 ? cells1 : cells10;

            // We create multiple queries and use a priority queue to return packets from each ship sorted by their
            // timestamp
            for (Cell c : cells) {
                queries.add(new AisStoreQuery(s, inner, batchLimit, table, COLUMN_CELLID, (int)c.getCellId(), startTimeInclusive, stopTimeExclusive));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }
        } else if (mmsi != null) {
            for (int m : mmsi) {
                queries.add(new AisStoreQuery(s, inner, batchLimit, TABLE_PACKETS_MMSI, COLUMN_MMSI, m, startTimeInclusive, stopTimeExclusive));
            }
        } else {
            int start = AisStoreSchema.timeBlock(TABLE_PACKETS_TIME, startTimeInclusive);
            int stop = AisStoreSchema.timeBlock(TABLE_PACKETS_TIME, stopTimeExclusive.minusMillis(1));
            queries.add(new AisStoreQuery(s, inner, batchLimit, TABLE_PACKETS_TIME, COLUMN_TIMEBLOCK, start, stop, startTimeInclusive, stopTimeExclusive));
        }
        return new AisStoreQueryResult(inner, queries);

    }

    public AisStoreQueryBuilder setFetchSize(int limit) {
        this.batchLimit = limit;
        return this;
    }

    public AisStoreQueryBuilder setInterval(Interval interval) {
        return setInterval(interval.getStartMillis(), interval.getEndMillis());
    }

    /**
     * @param startMillies
     *            the start date (inclusive)
     * @param stopMillies
     *            the end date (exclusive)
     * @return
     */
    public AisStoreQueryBuilder setInterval(Instant startMillies, Instant stopMillies) {
        this.startTimeInclusive = startMillies;
        this.stopTimeExclusive = stopMillies;
        return this;
    }

    /**
     * @param startMillies
     *            the start date (inclusive)
     * @param stopMillies
     *            the end date (exclusive)
     * @return
     * @deprecated
     */
    public AisStoreQueryBuilder setInterval(long startMillies, long stopMillies) {
        setInterval(Instant.ofEpochMilli(startMillies), Instant.ofEpochMilli(stopMillies));
        return this;
    }

    /**
     * Creates a new builder for packets received from within the specified area in the given interval.
     * 
     * @param area
     *            the area
     * @return a query builder
     * @throws NullPointerException
     *             if the specified area is null
     */
    public static AisStoreQueryBuilder forArea(Area area) {
        return new AisStoreQueryBuilder(requireNonNull(area), null);
    }

    /**
     * Finds all packets for one or more MMSI numbers.
     * 
     * @param mmsi
     *            one or more MMSI numbers
     * @return a query builder
     */
    public static AisStoreQueryBuilder forMmsi(int... mmsi) {
        if (mmsi.length == 0) {
            throw new IllegalArgumentException("Must request at least 1 mmsi number");
        }
        return new AisStoreQueryBuilder(null, mmsi);
    }

    /**
     * Finds all packets. Should be used together with {@link #setInterval(long, long)} to limit the amount of data to
     * return.
     * 
     * @return a query builder
     */
    public static AisStoreQueryBuilder forTime() {
        return new AisStoreQueryBuilder(null, null);
    }
}
