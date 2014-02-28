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
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

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
public class Monitor implements Consumer<AisPacket> {

    private Session viewSession;
    private Cluster viewCluster;
    private static final Logger LOG = Logger.getLogger(Monitor.class);

    private boolean dummy;

    AtomicInteger count = new AtomicInteger();

    StatisticsWriter stat;

    FarFutureFilter farFuture = new FarFutureFilter();
    FutureFilter future = new FutureFilter();
    
    TreeSet<RegularStatement> tree = new TreeSet<>(new Comparator<RegularStatement>() {
        @Override
        public int compare(RegularStatement o1, RegularStatement o2) {
            return o1.getQueryString().compareTo(o2.getQueryString());
        }
    });
    
    //TreeSet<RegularStatement> tree = new TreeSet<>();

    
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

    }

    @Override
    public void accept(AisPacket t) {
        if (count.getAndIncrement() % 10000 == 0) {
            stat.setEndTime(System.currentTimeMillis());
            stat.print();
        }
        if (dummy || t == null || recentPacket.rejectedByFilter(t)) {
            return;
        }

        long timeblock = t.getBestTimestamp() / 10 / 60 / 1000;
        final ConcurrentSkipListMap<String, Object> m = new ConcurrentSkipListMap<>();
        m.put(AisMatSchema.STREAM_TIME_KEY, timeblock);
        
        // quick implementation, could be cleaner
        if ("tree".equals(monitorType)) {
            tree.add(getStatement(AisMatSchema.TABLE_STREAM_MONITOR,m));
            
            if (count.get() % 1000 == 0) {
                for (RegularStatement s: tree.toArray(new RegularStatement[0])) {
                   LOG.debug(s.getQueryString()); 
                }
                viewSession.execute(QueryBuilder.batch(tree.toArray(new RegularStatement[0])));
                tree.clear();
            }            
        } else {
            viewSession.execute(getStatement(AisMatSchema.TABLE_STREAM_MONITOR, m));  
        }
        
    }

    public RegularStatement getStatement(String tableName, Map<String, Object> keys) {
        Insert insert = QueryBuilder.insertInto(tableName);
        insert.setConsistencyLevel(ConsistencyLevel.ANY);

        for (Entry<String, Object> e : keys.entrySet()) {
            insert.value(e.getKey(), e.getValue());
        }

        return insert;
    }
    
    public void insert(RegularStatement s) {
        viewSession.execute(s);
    }

    
    

}
