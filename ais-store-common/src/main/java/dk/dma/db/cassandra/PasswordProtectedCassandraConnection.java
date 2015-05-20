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
package dk.dma.db.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.SocketOptions;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A username/password protected connection to Cassandra.
 * 
 * @author Thomas Borg Salling
 */
public class PasswordProtectedCassandraConnection extends CassandraConnection {

    /** {@inheritDoc} */
    protected PasswordProtectedCassandraConnection(Cluster cluster, String keyspace) {
        super(cluster, keyspace);
    }

    /** {@inheritDoc} */
    public static PasswordProtectedCassandraConnection create(String username, String password, String keyspace, String... connectionPoints) {
        return create(username, password, keyspace, Arrays.asList(connectionPoints));
    }

    /** {@inheritDoc} */
    public static PasswordProtectedCassandraConnection create(String username, String password, String keyspace, List<String> connectionPoints) {
        Cluster cluster;

        if (seedsContainPortNumbers(connectionPoints)) {
            Collection<InetSocketAddress> cassandraSeeds = new ArrayList<>();
            connectionPoints.forEach(cp -> cassandraSeeds.add(connectionPointToInetAddress(cp)));
            cluster = Cluster.builder()
                .addContactPointsWithPorts(cassandraSeeds)
                .withCredentials(username, password)
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(1000*60))
                .build();
        } else {
            cluster = Cluster.builder()
                .addContactPoints(connectionPoints.toArray(new String[0]))
                .withCredentials(username, password)
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(1000*60))
                .build();
        }

        return new PasswordProtectedCassandraConnection(cluster, keyspace);
    }

}
