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

import static dk.dma.ais.store.AisStoreSchema.TABLE_AREA_CELL1;
import static dk.dma.ais.store.AisStoreSchema.TABLE_AREA_CELL10;
import static dk.dma.ais.store.AisStoreSchema.TABLE_AREA_CELL10_KEY;
import static dk.dma.ais.store.AisStoreSchema.TABLE_AREA_CELL1_KEY;
import static dk.dma.ais.store.AisStoreSchema.TABLE_MMSI;
import static dk.dma.ais.store.AisStoreSchema.TABLE_MMSI_KEY;
import static dk.dma.ais.store.AisStoreSchema.TABLE_TIME;
import static dk.dma.ais.store.AisStoreSchema.TABLE_TIME_KEY;
import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.AbstractIterator;
import com.google.common.primitives.Longs;

import dk.dma.ais.packet.AisPacket;
import dk.dma.commons.util.Iterators;
import dk.dma.enav.model.geometry.Area;
import dk.dma.enav.model.geometry.grid.Cell;
import dk.dma.enav.model.geometry.grid.Grid;

/**
 * This class implements the actual query.
 * 
 * @author Kasper Nielsen
 */
class AisStoreQuery extends AbstractIterator<AisPacket> {

    /**
     * AisPacket implements Comparable. But I'm to afraid someone might break the functionality someday So we make a
     * manual iterator
     * */
    static final Comparator<AisPacket> COMPARATOR = new Comparator<AisPacket>() {
        public int compare(AisPacket p1, AisPacket p2) {
            return Long.compare(p1.getBestTimestamp(), p2.getBestTimestamp());
        }
    };

    /** The number of results to get at a time. */
    private static final int LIMIT = 3000; // magic constant found by trial

    /** The session used for querying. */
    private final Session session;

    /** The name of the table we are querying. */
    private final String tableName;

    /** The name of the key row in the table. */
    private final String rowName;

    /**
     * The first timestamp for which to get packets (inclusive). Is constantly updated to the timestamp of the last
     * received packet as data comes in.
     */
    private ByteBuffer timeStart;

    /** The last timestamp for which to get packets (exclusive) */
    private final ByteBuffer timeStop;

    private int currentRow;
    private final int lastRow;

    /** All queries are done asynchronously. This future holds the result of the last query we made. */
    private ResultSetFuture future;

    /** A list of packets that we have received from AisStore but have not yet returned to the user. */
    private LinkedList<AisPacket> packets = new LinkedList<>();

    AisStoreQuery(Session session, String tableName, String rowName, int rowStart, long timeStartInclusive,
            long timeStopExclusive) {
        this(session, tableName, rowName, rowStart, rowStart, timeStartInclusive, timeStopExclusive);
    }

    AisStoreQuery(Session session, String tableName, String rowName, int rowStart, int rowStop,
            long timeStartInclusive, long timeStopExclusive) {
        this.session = requireNonNull(session);
        this.tableName = requireNonNull(tableName);
        this.rowName = requireNonNull(rowName);
        this.currentRow = rowStart;
        this.lastRow = rowStop;
        this.timeStart = ByteBuffer.wrap(Longs.toByteArray(timeStartInclusive));
        this.timeStop = ByteBuffer.wrap(Longs.toByteArray(timeStopExclusive));
        advance();
    }

    public AisPacket computeNext() {
        AisPacket next = packets.poll();
        if (next != null) {
            return next;
        }
        while (currentRow <= lastRow) {
            List<Row> all;
            try {
                all = future.get().all();
                future = null; // make sure we do not use the same future again
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // advance to next row, we did not get a complete result set
            if (all.size() < LIMIT) {
                currentRow++;
            }
            if (all.size() > 0) {
                Row row = all.get(all.size() - 1);
                timeStart = ByteBuffer.wrap(ByteBufferUtil.getArray(row.getBytes(0)));
            }
            advance(); // make sure to fetch next before we start the parsing

            for (Row row : all) {
                packets.add(AisPacket.fromByteBuffer(row.getBytes(1)));
            }

            if (!packets.isEmpty()) {
                return packets.poll();
            }
        }
        return endOfData();
    }

    /** If the current row is less than or equal to the current row. We will fetch the next data. */
    void advance() {
        if (currentRow <= lastRow) {
            // We need timehash to find out what the timestamp of the last received packet is.
            // When the datastax driver supports unlimited fetching we will only need aisdata
            Select s = QueryBuilder.select("timehash", "aisdata").from(tableName);
            Where w = s.where(QueryBuilder.eq(rowName, currentRow));
            w.and(QueryBuilder.gt("timehash", timeStart)); // timehash must be greater than start
            w.and(QueryBuilder.lt("timehash", timeStop)); // timehash must be less that stop
            s.limit(LIMIT); // Sets the limit
            // System.out.println(s.getQueryString());
            future = session.executeAsync(s.getQueryString());
        }
    }

    static AisStoreQueryResult forTime(final Session s, final long startInclusive, final long stopExclusive) {
        requireNonNull(s);
        final int start = AisStoreSchema.getTimeBlock(startInclusive);
        final int stop = AisStoreSchema.getTimeBlock(stopExclusive - 1);

        return new AisStoreQueryResult() {
            public Iterator<AisPacket> createQuery() {
                return new AisStoreQuery(s, TABLE_TIME, TABLE_TIME_KEY, start, stop, startInclusive, stopExclusive);
            }
        };
    }

    static AisStoreQueryResult forMmsi(final Session s, final long startInclusive, final long stopExclusive,
            final int... mmsi) {
        requireNonNull(s);
        if (mmsi.length == 0) {
            throw new IllegalArgumentException("Must request at least 1 mmsi number");
        }

        // We create multiple queries and use a priority queue to return packets from each ship sorted by their
        // timestamp
        return new AisStoreQueryResult() {
            public Iterator<AisPacket> createQuery() {
                ArrayList<AisStoreQuery> queries = new ArrayList<>();
                for (int m : mmsi) {
                    queries.add(new AisStoreQuery(s, TABLE_MMSI, TABLE_MMSI_KEY, m, startInclusive, stopExclusive));
                }
                return Iterators.combine(queries, COMPARATOR);// Return the actual iterator, if queries only contains 1
            }
        };
    }

    static AisStoreQueryResult forArea(final Session s, Area area, final long startInclusive, final long stopExclusive) {
        requireNonNull(s);
        Set<Cell> cells1 = Grid.GRID_1_DEGREE.getCells(area);
        Set<Cell> cells10 = Grid.GRID_10_DEGREES.getCells(area);

        int factor = 10;// magic constant

        // Determines if use the tables of size 1 degree, or size 10 degrees
        boolean useCell1 = cells10.size() * factor > cells1.size();
        final String tableName = useCell1 ? TABLE_AREA_CELL1 : TABLE_AREA_CELL10;
        final String keyName = useCell1 ? TABLE_AREA_CELL1_KEY : TABLE_AREA_CELL10_KEY;
        final Set<Cell> cells = useCell1 ? cells1 : cells10;

        // We create multiple queries and use a priority queue to return packets from each ship sorted by their
        // timestamp
        return new AisStoreQueryResult() {
            public Iterator<AisPacket> createQuery() {
                ArrayList<AisStoreQuery> queries = new ArrayList<>();
                for (Cell c : cells) {
                    queries.add(new AisStoreQuery(s, tableName, keyName, c.getCellId(), startInclusive, stopExclusive));
                }
                return Iterators.combine(queries, COMPARATOR);
            }
        };
    }
}
