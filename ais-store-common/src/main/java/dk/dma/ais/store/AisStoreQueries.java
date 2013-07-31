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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
 * 
 * @author Kasper Nielsen
 */
class AisStoreQueries extends AbstractIterator<AisPacket> {
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
    private static final int LIMIT = 3000; // found by trial

    /** The session used for querying. */
    private final Session session;

    /** The name of the table we are querying. */
    private final String tableName;

    /** The name of the key row in the table. */
    private final String rowName;

    private ByteBuffer timeStart;
    private final ByteBuffer timeStop;

    private int currentRow;
    private final int lastRow;

    private ResultSetFuture future;

    private Queue<AisPacket> packets = new LinkedList<>();

    AisStoreQueries(Session session, String tableName, String rowName, int rowStart, long timeStartInclusive,
            long timeStopExclusive) {
        this(session, tableName, rowName, rowStart, rowStart, timeStartInclusive, timeStopExclusive);
    }

    AisStoreQueries(Session session, String tableName, String rowName, int rowStart, int rowStop,
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
            for (int i = 0; i < all.size(); i++) {
                Row row = all.get(i);
                ByteBuffer packet = row.getBytes(1);
                packets.add(AisPacket.fromByteBuffer(packet));
                if (i == all.size() - 1) { // find out where the next query should start
                    timeStart = ByteBuffer.wrap(ByteBufferUtil.getArray(row.getBytes(0)));
                }
            }
            advance(); // make sure to fetch next
            if (!packets.isEmpty()) {
                return packets.poll();
            }
        }
        return endOfData();
    }

    void advance() {
        if (currentRow <= lastRow) {
            Select s = QueryBuilder.select("timehash", "aisdata").from(tableName);
            Where w = s.where(QueryBuilder.eq(rowName, currentRow));
            w.and(QueryBuilder.gt("timehash", timeStart));
            w.and(QueryBuilder.lt("timehash", timeStop));
            s.limit(LIMIT);
            // System.out.println(s.getQueryString());
            future = session.executeAsync(s.getQueryString());
        }
    }

    public static Iterable<AisPacket> forTime(final Session s, final long startInclusive, final long stopExclusive) {
        requireNonNull(s);
        final int start = AisStoreSchema.getTimeBlock(startInclusive);
        final int stop = AisStoreSchema.getTimeBlock(stopExclusive - 1);
        return new Iterable<AisPacket>() {
            public Iterator<AisPacket> iterator() {
                return new AisStoreQueries(s, AisStoreSchema.TABLE_TIME, AisStoreSchema.TABLE_TIME_KEY, start, stop,
                        startInclusive, stopExclusive);
            }
        };
    }

    public static Iterable<AisPacket> forMmsi(final Session s, final long startInclusive, final long stopExclusive,
            final int... mmsi) {
        requireNonNull(s);
        if (mmsi.length == 0) {
            return Collections.emptyList();
        } else if (mmsi.length == 1) {
            return new Iterable<AisPacket>() {
                public Iterator<AisPacket> iterator() {
                    return new AisStoreQueries(s, AisStoreSchema.TABLE_MMSI, AisStoreSchema.TABLE_MMSI_KEY, mmsi[0],
                            startInclusive, stopExclusive);
                }
            };
        }
        return new Iterable<AisPacket>() {
            public Iterator<AisPacket> iterator() {
                ArrayList<AisStoreQueries> queries = new ArrayList<>();
                for (int i = 0; i < mmsi.length; i++) {
                    queries.add(new AisStoreQueries(s, AisStoreSchema.TABLE_MMSI, AisStoreSchema.TABLE_MMSI_KEY,
                            mmsi[i], startInclusive, stopExclusive));
                }
                return Iterators.combine(queries, COMPARATOR);
            }
        };
    }

    public static Iterable<AisPacket> forArea(final Session s, Area area, final long startInclusive,
            final long stopExclusive) {
        requireNonNull(s);
        Set<Cell> cells1 = Grid.GRID_1_DEGREE.getCells(area);
        Set<Cell> cells10 = Grid.GRID_10_DEGREES.getCells(area);
        int factor = 10;// magic constant
        boolean useCell1 = cells10.size() * factor > cells1.size();
        final String tableName = useCell1 ? AisStoreSchema.TABLE_AREA_CELL1 : AisStoreSchema.TABLE_AREA_CELL10;
        final String keyName = useCell1 ? AisStoreSchema.TABLE_AREA_CELL1_KEY : AisStoreSchema.TABLE_AREA_CELL10_KEY;
        final Set<Cell> cells = useCell1 ? cells1 : cells10;
        if (cells.size() == 1) {
            return new Iterable<AisPacket>() {
                public Iterator<AisPacket> iterator() {
                    return new AisStoreQueries(s, tableName, keyName, cells.iterator().next().getCellId(),
                            startInclusive, stopExclusive);
                }
            };
        }
        return new Iterable<AisPacket>() {
            public Iterator<AisPacket> iterator() {
                ArrayList<AisStoreQueries> queries = new ArrayList<>();
                for (Cell c : cells) {
                    queries.add(new AisStoreQueries(s, tableName, keyName, c.getCellId(), startInclusive, stopExclusive));
                }
                return Iterators.combine(queries, COMPARATOR);
            }
        };
    }
}
