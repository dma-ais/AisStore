/*
 * Copyright (c) 2008 Kasper Nielsen.
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
package dk.dma.app.cassandra;

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

import dk.dma.app.service.AbstractBatchedStage;

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
        this.keyspace = context.getEntity();
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
