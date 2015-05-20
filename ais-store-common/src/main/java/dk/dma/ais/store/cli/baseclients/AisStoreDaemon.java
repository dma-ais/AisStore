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
package dk.dma.ais.store.cli.baseclients;

import com.beust.jcommander.Parameter;
import dk.dma.commons.app.AbstractDaemon;
import dk.dma.commons.management.ManagedResource;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.db.cassandra.PasswordProtectedCassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static dk.dma.commons.util.EnvironmentUtil.env;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * An abstract base class for Daemon processes which can provide a CassandraConnection
 * based on command line arguments.
 *
 * @author Thomas Borg Salling
 */
@ManagedResource
public abstract class AisStoreDaemon extends AbstractDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(AisStoreDaemon.class);
    { LOG.info("AisStoreDaemon created."); }

    private final static String ENV_KEY_AISSTORE_USER = "AISSTORE_USER";
    private final static String ENV_KEY_AISSTORE_PASS = "AISSTORE_PASS";

    @Parameter(names = "-secure", description = "Use $AISSTORE_USER and $AISSTORE_PASS as username/password for Cassandra")
    boolean secureConnection = false;

    @Parameter(names = "-keyspace", description = "The Cassandra keyspace to read/write data from/to")
    String keyspaceName = "aisdata";

    @Parameter(names = "-seeds", description = "A list of Cassandra hosts used to bootstrap the connection to the database cluster, list=empty -> AisStore disabled")
    List<String> seeds = Arrays.asList("localhost");

    /**
     * Create a new connection to AisStore and start it.
     *
     * @return A new and started connection to AisStore. Null if no seeds or database name are provided.
     */
    public CassandraConnection connect() {
        CassandraConnection connection = connect(seeds, keyspaceName, secureConnection);
        return connection == null ? null : start(connection);
    }

    /**
     * Create a new (non-started) connection to AisStore.
     *
     * @return A new connection to AisStore. Null if no seeds or database name are provided.
     */
    static CassandraConnection connect(List<String> seeds, String keyspace, boolean secure) {
        CassandraConnection con;
        if (seeds != null && seeds.size() > 0 && !isBlank(keyspace)) {
            if (secure) {
                LOG.info("Creating secure Cassandra connection.");
                con = PasswordProtectedCassandraConnection.create(env(ENV_KEY_AISSTORE_USER), env(ENV_KEY_AISSTORE_PASS), keyspace, seeds);
                LOG.info("Connected to Cassandra cluster " + con.getSession().getCluster().getClusterName());
            } else {
                LOG.info("Creating unsecure Cassandra connection.");
                con = CassandraConnection.create(keyspace, seeds);
            }
        } else {
            LOG.warn("No seeds or keyspace for AisStore. No connection established.");
            con = null;
        }
        return con;
    }
}
