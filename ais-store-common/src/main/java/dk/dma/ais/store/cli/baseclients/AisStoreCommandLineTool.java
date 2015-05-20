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
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.management.ManagedResource;
import dk.dma.db.cassandra.CassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * An abstract base class for CLI tools which can provide a CassandraConnection
 * based on command line arguments.
 *
 * @author Thomas Borg Salling
 */
@ManagedResource
public abstract class AisStoreCommandLineTool extends AbstractCommandLineTool {

    private static final Logger LOG = LoggerFactory.getLogger(AisStoreCommandLineTool.class);
    { LOG.info("AisStoreCommandLineTool created."); }

    @Parameter(names = "-secure", description = "Use $AISSTORE_USER and $AISSTORE_PASS as username/password for Cassandra")
    boolean secureConnection = false;

    @Parameter(names = "-keyspace", description = "The Cassandra keyspace to read/write data from/to")
    String keyspaceName = "aisdata";

    @Parameter(names = "-seeds", description = "A list of Cassandra hosts used to bootstrap the connection to the database cluster, list=empty -> AisStore disabled")
    List<String> seeds = Arrays.asList("localhost");

    /**
     * Create a new connection to AisStore and start it.
     * @return A new and started connection to AisStore. Null if no seeds or database name are provided.
     */
    public CassandraConnection connect() {
        CassandraConnection connection = AisStoreDaemon.connect(seeds, keyspaceName, secureConnection);
        return connection == null ? null : start(connection);
    }

}
