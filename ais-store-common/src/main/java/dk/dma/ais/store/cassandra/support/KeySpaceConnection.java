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
package dk.dma.ais.store.cassandra.support;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.AbstractService;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import dk.dma.commons.service.AbstractBatchedStage;

/**
 * 
 * @author Kasper Nielsen
 */
public class KeySpaceConnection extends AbstractService {
    public static final String DEFAULT_CLUSTER_NAME = "DmaCluster";

    private final AstyanaxContext<Keyspace> context;

    private final Keyspace keyspace;

    private final ExecutorService es = Executors.newCachedThreadPool();

    KeySpaceConnection(String cluster, String keyspace, String hostAndPorts) {
        context = new AstyanaxContext.Builder()
                .forCluster(cluster)
                .forKeyspace(keyspace)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl().setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(3).setSeeds(
                                hostAndPorts)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        this.keyspace = context.getClient();
    }

    public <T> AbstractBatchedStage<T> createdBatchedStage(int batchSize, final CassandraWriteSink<T> writer) {
        return new CassandraBatchedStagedWriter<>(this, batchSize, writer);
    }

    /** {@inheritDoc} */
    @Override
    protected void doStart() {
        context.start();
        notifyStarted();
    }

    /** {@inheritDoc} */
    @Override
    protected void doStop() {
        context.shutdown();
        es.shutdown();
        try {
            es.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        notifyStopped();
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public <K, C> ColumnFamilyQuery<K, C> prepareQuery(ColumnFamily<K, C> cf) {
        return keyspace.prepareQuery(cf);
    }

    public static KeySpaceConnection connect(String keyspace, Iterable<String> hostAndPorts) {
        return connect(keyspace, Joiner.on(", ").join(hostAndPorts));
    }

    public static KeySpaceConnection connect(String keyspace, String hostAndPorts) {
        return connect(DEFAULT_CLUSTER_NAME, keyspace, hostAndPorts);
    }

    public static KeySpaceConnection connect(String cluster, String keyspace, String hostAndPorts) {
        return new KeySpaceConnection(cluster, keyspace, hostAndPorts);
    }
}
