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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.AbstractIterator;
import com.google.common.primitives.Longs;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreSchema.Column;
import dk.dma.ais.store.AisStoreSchema.Table;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_AISDATA;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMEBLOCK;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMESTAMP;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_TIME;
import static java.util.Objects.requireNonNull;

/**
 * This will generate one complete query for a data range
 * 
 * @author Jens Tuxen
 */
class AisStoreQuery extends AbstractIterator<AisPacket> {

    /**
     * AisPacket implements Comparable. But I'm to afraid someone might break
     * the functionality someday. So we make a manual iterator
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
    private final Table table;

    /** The name of the key row in the table. */
    private final Column rowName;

    /**
     * The first timestamp for which to get packets (inclusive). Is constantly
     * updated to the timestamp of the last received packet as data comes in.
     */
    private Instant timeStart;

    /** The last timestamp for which to get packets (exclusive) */
    private final Instant timeStop;

    private int currentRow;

    private final int lastRow;

    /**
     * All queries are done asynchronously. This future holds the result of the
     * last query we made.
     */
    private ResultSetFuture future;

    /**
     * A list of packets that we have received from AisStore but have not yet
     * returned to the user.
     */
    private LinkedList<AisPacket> packets = new LinkedList<>();

    /** The number of packets that was retrieved. */
    private volatile long retrievedPackets;

    private volatile long lastestDateReceived;

    private final AisStoreQueryInnerContext inner;

    private Iterator<Row> it;
    private ResultSet rs;

    AisStoreQuery(Session session, AisStoreQueryInnerContext inner,
                  int batchLimit, Table table, Column rowName, int rowStart,
                  Instant timeStartInclusive, Instant timeStopExclusive) {
        this(session, inner, batchLimit, table, rowName, rowStart,
                rowStart, timeStartInclusive, timeStopExclusive);
    }

    AisStoreQuery(Session session, AisStoreQueryInnerContext inner,
                  int batchLimit, Table table, Column rowName, int rowStart,
                  int rowStop, Instant timeStartInclusive, Instant timeStopExclusive) {
        this.session = requireNonNull(session);
        this.table = requireNonNull(table);
        this.rowName = requireNonNull(rowName);
        this.currentRow = rowStart;
        this.lastRow = rowStop;
        this.batchLimit = batchLimit;
        this.timeStart = timeStartInclusive;
        this.timeStop = timeStopExclusive;
        this.inner = inner;

        execute();
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
            Row row = null;
            int innerReceived = 0;
            while (it.hasNext() && innerReceived < batchLimit) {

                // optimistic automatic-paging+fetch
                if (rs.getAvailableWithoutFetching() == 100
                        && !rs.isFullyFetched()) {
                    rs.fetchMoreResults();
                }

                row = it.next();
                packets.add(AisPacket.fromByteBuffer(row.getBytes(1)));
                retrievedPackets++;
                innerReceived++;
            }

            if (innerReceived > 0) {
                ByteBuffer buf = row.getBytes(0);
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);

                lastestDateReceived = Longs.fromByteArray(bytes);

                // currentRow == lastRow when packets_mmsi or packets_cell
                if (!(currentRow == lastRow)) {
                    currentRow = AisStoreSchema.getTimeBlock(table, Instant.ofEpochMilli(lastestDateReceived));
                    // System.out.println("Currently at: "+currentRow+" Last ROW is: "+lastRow);
                }

                System.out.println("Currently at: "
                        + new Date(lastestDateReceived));

            }

            if (!packets.isEmpty()) {
                return packets.poll();
            }

            if (rs.isFullyFetched() || future.isDone()) {
                currentRow = lastRow + 1;
                inner.finished(this);
                return endOfData();
            }

        }

        inner.finished(this);
        return endOfData();
    }

    private static List<Integer> timeBlocks(Table table, Instant timeStart, Instant timeStop) {
        int timeBlockMin = AisStoreSchema.getTimeBlock(table, timeStart);
        int timeBlockMax = AisStoreSchema.getTimeBlock(table, timeStop);

        List<Integer> timeBlocks = new ArrayList<>(timeBlockMax - timeBlockMin + 1);
        for (int timeBlock = timeBlockMin; timeBlock <= timeBlockMax; timeBlock++)
            timeBlocks.add(timeBlock);

        return timeBlocks;
    }

    /** execute takes over from advance, which is not necessary anymore */
    void execute() {
        List<Integer> timeBlocks = timeBlocks(table, timeStart, timeStop);

        Statement statement;
        switch (table) {
        case TABLE_PACKETS_TIME:
            statement = QueryBuilder
                .select(COLUMN_TIMESTAMP.toString(), COLUMN_AISDATA.toString())
                .from(TABLE_PACKETS_TIME.toString())
                .where(in(COLUMN_TIMEBLOCK.toString(), timeBlocks))
                .and(gt(COLUMN_TIMESTAMP.toString(), timeStart))
                .and(lt(COLUMN_TIMESTAMP.toString(), timeStop));
            break;
        default:
            statement = QueryBuilder
                .select(COLUMN_TIMESTAMP.toString(), COLUMN_AISDATA.toString())
                .from(table.toString())
                .where(eq(rowName.toString(), currentRow))
                .and(in(COLUMN_TIMEBLOCK.toString(), timeBlocks))
                .and(gt(COLUMN_TIMESTAMP.toString(), timeStart))
                .and(lt(COLUMN_TIMESTAMP.toString(), timeStop));
            break;
        }

        statement.setFetchSize(batchLimit);
        statement.setConsistencyLevel(ConsistencyLevel.ONE);

        Select select = (Select) statement;
        select.limit(Integer.MAX_VALUE); // Sets the limit

        System.out.println(select.getQueryString());
        future = session.executeAsync(select);
        rs = future.getUninterruptibly();
        it = rs.iterator();
    }

}
