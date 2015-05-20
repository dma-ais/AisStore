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

/**
 * An abstract base class for Daemon processes which can provide a CassandraConnection
 * based on command line arguments.
 *
 * @author Thomas Borg Salling
 */
@ManagedResource
public abstract class AisStoreDaemon extends AbstractDaemon {

    /** The logger. */
    private static final Logger LOG = LoggerFactory.getLogger(AisStoreDaemon.class);
    { LOG.info("AisStoreDaemon created."); }

    private final static String ENV_KEY_AISSTORE_USER = "AISSTORE_USER";
    private final static String ENV_KEY_AISSTORE_PASS = "AISSTORE_PASS";

    @Parameter(names = "-secure", description = "Use $AISSTORE_USER and $AISSTORE_PASS as username/password for Cassandra")
    boolean useSecureCassandraConnection = false;

    @Parameter(names = "-databaseName", description = "The Cassandra database to write data to")
    String databaseName = "aisdata";

    @Parameter(names = "-database", description = "A list of Cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    /**
     * Create a new connection to AisStore and start it.
     * @return A new and started connection to AisStore.
     */
    public CassandraConnection connect() {
        CassandraConnection con;
        if (useSecureCassandraConnection) {
            LOG.info("Starting secure Cassandra connection.");
            con = start(PasswordProtectedCassandraConnection.create(env(ENV_KEY_AISSTORE_USER), env(ENV_KEY_AISSTORE_PASS), databaseName, cassandraSeeds));
        } else {
            LOG.info("Starting unsecure Cassandra connection.");
            con = start(CassandraConnection.create(databaseName, cassandraSeeds));
        }
        return con;
    }
}
