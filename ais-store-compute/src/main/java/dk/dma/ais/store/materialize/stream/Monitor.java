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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;




import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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
public class Monitor implements Consumer<AisPacket>{

    private Session viewSession;
    private Cluster viewCluster;
    private static final Logger LOG = Logger.getLogger(Monitor.class);
    
    private boolean dummy;
    
    AtomicInteger count = new AtomicInteger();

    StatisticsWriter stat; 
    
    public Monitor(CassandraConnection con, Cluster viewCluster,
            Session viewSession, boolean dummy, PrintWriter pw) {
        super();

        this.viewCluster = viewCluster;
        this.viewSession = viewSession;
        this.dummy = dummy;
        
        stat = new StatisticsWriter(count, this, pw);

    }

    @Override
    public void accept(AisPacket t) {
    	if (count.getAndIncrement() % 100000 == 0) {
    		stat.print();
    	}
    	
    	if (dummy) {
    		return;
    	}
    	
        long timeblock = t.getBestTimestamp()/10/60/1000;
        final ConcurrentSkipListMap<String, Object> m = new ConcurrentSkipListMap<>();
        m.put(AisMatSchema.STREAM_TIME_KEY, timeblock);     
        this.insert(AisMatSchema.TABLE_STREAM_MONITOR, m);
    }

    
    public void insert(String tableName, Map<String, Object> keys) {
        Insert insert = QueryBuilder.insertInto(tableName);
        insert.setConsistencyLevel(ConsistencyLevel.ANY);

        for (Entry<String, Object> e : keys.entrySet()) {
            insert.value(e.getKey(), e.getValue());
        }

        LOG .debug(insert);
        viewSession.execute(insert);
    }
    
 
    
    

}
