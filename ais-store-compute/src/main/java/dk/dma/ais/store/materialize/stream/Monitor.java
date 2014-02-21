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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.OrderedViewBuilder;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.util.function.Consumer;

/**
 * A Fate-share stream monitor for AisStore which monitors the stream
 * 
 * @author Jens Tuxen
 */
public class Monitor implements Consumer<AisPacket>, OrderedViewBuilder {

    private static final Logger LOG = Logger.getLogger(Monitor.class);
    protected Cluster viewCluster;
    protected Session viewSession;

    public Monitor(List<String> viewHosts) {
        this.viewCluster = Cluster.builder()
                .addContactPoints(viewHosts.toArray(new String[0])).build();
        this.viewSession = viewCluster.connect("aismat");
    }

    public Monitor(CassandraConnection con, Cluster viewCluster,
            Session viewSession) {
        super();

        this.viewCluster = viewCluster;
        this.viewSession = viewSession;
    }

    @Override
    public void accept(AisPacket t) {
        long timestamp = t.getBestTimestamp() / 1000;

        final ConcurrentSkipListMap<String, Object> m = new ConcurrentSkipListMap<>();
        m.put(AisMatSchema.TIME_KEY, timestamp);

        this.increment("stream_event", m);
    }

    @Override
    public void increment(String tableName, Map<String, Object> keys) {
        Update upd = QueryBuilder.update(tableName);
        upd.setConsistencyLevel(ConsistencyLevel.ANY);

        for (Entry<String, Object> e : keys.entrySet()) {
            upd.where(QueryBuilder.eq(e.getKey(), e.getValue().toString()));
        }
        upd.with(QueryBuilder.incr(AisMatSchema.RESULT_KEY));

        LOG.debug(upd);
        //viewSession.execute(upd);
    }

}
