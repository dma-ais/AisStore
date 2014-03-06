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
package dk.dma.ais.store.materialize.stream;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import dk.dma.ais.filter.FarFutureFilter;
import dk.dma.ais.filter.FutureFilter;
import dk.dma.ais.filter.IPacketFilter;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.util.StatisticsWriter;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.util.function.Consumer;

/**
 * A Fate-share stream monitor for AisStore which monitors the stream
 * 
 * @author Jens Tuxen
 */
public class Monitor implements Consumer<AisPacket>, Runnable {

    private Session viewSession;
    private Cluster viewCluster;
    private static final Logger LOG = Logger.getLogger(Monitor.class);

    private boolean dummy;

    AtomicInteger count = new AtomicInteger();

    StatisticsWriter stat;

    FarFutureFilter farFuture = new FarFutureFilter();
    FutureFilter future = new FutureFilter();
    
    //atomical clearing of sortedSet requires a lock, we can't used synchronized because it's random access
    //that could potentially mean starvation of the Monitor.run() thread
    final ReentrantLock sortedSetLock = new ReentrantLock(true); 
    SortedSet<Integer> timeblocks = Collections.synchronizedSortedSet(new TreeSet<Integer>());
    
    //private final SimpleDateFormat parser = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
    @SuppressWarnings("deprecation")
    // TODO: need to implement a general solution for packet sanity
    final IPacketFilter recentPacket = new IPacketFilter()  {
        private final Long dec2013 = new Date(2013-1900,10,1,0,0).getTime();

        @Override
        public boolean rejectedByFilter(AisPacket packet) {
            final long timestamp = packet.getBestTimestamp();
            return timestamp < 0 || timestamp < dec2013
                    || farFuture.rejectedByFilter(packet)
                    || future.rejectedByFilter(packet);

        }
    };

    private String monitorType;

    public Monitor(CassandraConnection con, Cluster viewCluster,
            Session viewSession, boolean dummy, String monitorType, PrintWriter pw) {
        super();

        this.viewCluster = viewCluster;
        this.viewSession = viewSession;
        this.dummy = dummy;
        this.monitorType = monitorType;

        stat = new StatisticsWriter(count, this, pw);
        
        new Thread(this).start();
    }

    @Override
    public void accept(AisPacket t) {
        count.incrementAndGet();
        if (dummy || t == null || recentPacket.rejectedByFilter(t)) {
            return;
        }

        Integer timeblock = (int) (t.getBestTimestamp() / 10L / 60L / 1000L);
        
        sortedSetLock.lock();
        try {
            this.timeblocks.add(timeblock);
        } finally {
            sortedSetLock.unlock();
        }
    }


    
    public RegularStatement getStatement(String tableName, String key, Object value) {
        Insert insert = QueryBuilder.insertInto(tableName);
        insert.setConsistencyLevel(ConsistencyLevel.ANY);
        insert.value(key, value);
        return insert;
    }
    
    public void insert(RegularStatement s) {
        viewSession.execute(s);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(2000);              
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            if (!dummy) {
                sortedSetLock.lock();
                try {
                    ArrayList<RegularStatement> blocks = new ArrayList<>(timeblocks.size());
                    for (Integer timeblock: timeblocks) {
                        blocks.add(getStatement(AisMatSchema.TABLE_STREAM_MONITOR, "timeblock", timeblock));
                    }
                    viewSession.execute(QueryBuilder.batch(blocks.toArray(new RegularStatement[0])));                   
                    timeblocks.clear();
                } finally {
                    sortedSetLock.unlock();
                }    
            }
            
            stat.setEndTime(System.currentTimeMillis());
            stat.print();
        }
        
    }

    
    

}
