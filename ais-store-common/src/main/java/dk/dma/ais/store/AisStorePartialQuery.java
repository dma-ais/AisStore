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
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

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

/**
 * This class implements the actual query.
 * 
 * @author Kasper Nielsen
 */
class AisStorePartialQuery extends AbstractIterator<AisPacket> {

    /**
     * AisPacket implements Comparable. But I'm to afraid someone might break the functionality someday. So we make a
     * manual iterator
     * */
    static final Comparator<AisPacket> COMPARATOR = new Comparator<AisPacket>() {
        public int compare(AisPacket p1, AisPacket p2) {
            return Long.compare(p1.getBestTimestamp(), p2.getBestTimestamp());
        }
    };

    /** The number of results to get at a time. */
    private final int batchLimit;

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

    /** The number of packets that was retrieved. */
    private volatile long retrievedPackets;

    private volatile long lastestDateReceived;

    private final AisStoreQueryInnerContext inner;

    AisStorePartialQuery(Session session, AisStoreQueryInnerContext inner, int batchLimit, String tableName,
            String rowName, int rowStart, long timeStartInclusive, long timeStopExclusive) {
        this(session, inner, batchLimit, tableName, rowName, rowStart, rowStart, timeStartInclusive, timeStopExclusive);
    }

    AisStorePartialQuery(Session session, AisStoreQueryInnerContext inner, int batchLimit, String tableName,
            String rowName, int rowStart, int rowStop, long timeStartInclusive, long timeStopExclusive) {
        this.session = requireNonNull(session);
        this.tableName = requireNonNull(tableName);
        this.rowName = requireNonNull(rowName);
        this.currentRow = rowStart;
        this.lastRow = rowStop;
        this.batchLimit = batchLimit;
        this.timeStart = ByteBuffer.wrap(Longs.toByteArray(timeStartInclusive));
        this.timeStop = ByteBuffer.wrap(Longs.toByteArray(timeStopExclusive));
        advance();
        this.inner = inner;
        inner.queries.add(this);
    }

    long getNumberOfRetrievedPackets() {
        return retrievedPackets;
    }

    long getLatestRetrievedTimestamp() {
        return lastestDateReceived;
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
                inner.inner.setException(e);
                throw new RuntimeException(e);
            }

            // advance to next row, we did not get a complete result set
            if (all.size() < batchLimit) {
                currentRow++;
            }
            if (all.size() > 0) {
                retrievedPackets += all.size();
                Row row = all.get(all.size() - 1);
                byte[] bytes = ByteBufferUtil.getArray(row.getBytes(0));
                lastestDateReceived = Longs.fromByteArray(bytes);
                System.out.println(new Date(lastestDateReceived));
                timeStart = ByteBuffer.wrap(bytes);
            }
            advance(); // make sure to fetch next before we start the parsing

            for (Row row : all) {
                packets.add(AisPacket.fromByteBuffer(row.getBytes(1)));
            }

            if (!packets.isEmpty()) {
                return packets.poll();
            }
        }
        inner.finished(this);
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
            s.limit(batchLimit); // Sets the limit
            // System.out.println(s.getQueryString());
            future = session.executeAsync(s.getQueryString());
        }
    }

}
