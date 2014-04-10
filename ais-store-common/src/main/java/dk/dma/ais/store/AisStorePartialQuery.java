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
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
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
    
    private Iterator<Row> it;
    private ResultSet rs;


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
                //optimistic automatic-paging+fetch
                if (rs.getAvailableWithoutFetching() == batchLimit && !rs.isFullyFetched()) {
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
                
                //currentRow == lastRow when packets_mmsi for instance
                if (!(currentRow == lastRow)){
                    currentRow = AisStoreSchema.getTimeBlock(new Date(lastestDateReceived).getTime());
                    System.out.println("Currently at: "+currentRow+" Last ROW is: "+lastRow);
                }
                
                System.out.println("Currently at: "+new Date(lastestDateReceived));
                
            }
            
            
            
            
            if (!packets.isEmpty()) {
                return packets.poll();
            }
            
            if (rs.isFullyFetched() || future.isDone()) {
                System.out.println("WE ARE DONE");
                currentRow = lastRow+1;
                inner.finished(this);
                return endOfData();
            }
            
        }
        
        inner.finished(this);
        return endOfData();
    }

    /** execute takes over from advance, which is not necessary anymore */
    void execute() {
        // We need timehash to find out what the timestamp of the last received packet is.
        // When the datastax driver supports unlimited fetching we will only need aisdata
        Select s = QueryBuilder.select("timehash", "aisdata").from(tableName);
        Where w = null;
        switch(tableName) {
            case AisStoreSchema.TABLE_TIME:
                List<Integer>blocks = new ArrayList<>();
                int block = currentRow;
                while (block <= lastRow) {
                    blocks.add(block);
                    block++;
                }
                Integer[] blocksInt = blocks.toArray(new Integer[blocks.size()]);
                w = s.where(QueryBuilder.in(rowName,(Object[])blocksInt)); //timehash must be greater than start
                break;
            default:
                w = s.where(QueryBuilder.eq(rowName, currentRow));
                break;
        }
        
        w.and(QueryBuilder.gt("timehash", timeStart));
        w.and(QueryBuilder.lt("timehash", timeStop)); // timehash must be less that stop
        s.limit(Integer.MAX_VALUE); // Sets the limit
        System.out.println(s.getQueryString());
        future = session.executeAsync(s);
        rs = future.getUninterruptibly();
        it = rs.iterator();
    }

}
