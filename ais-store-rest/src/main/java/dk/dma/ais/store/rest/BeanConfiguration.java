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
package dk.dma.ais.store.rest;

import com.google.common.util.concurrent.Service;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.db.cassandra.PasswordProtectedCassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Configuration
public class BeanConfiguration {

    static final Logger LOG = LoggerFactory.getLogger(BeanConfiguration.class);

    {  LOG.info("BeanConfiguration created."); }

    /** Name of Cassandra keyspace */
    @Value("${dk.dma.ais.store.rest.cassandra.keyspace}")
    private String cassandraKeyspace;

    /** Cassandra seed nodes */
    @Value("#{'${dk.dma.ais.store.rest.cassandra.seeds}'.split(',')}")
    private List<String> cassandraContactPoints;

    /** Cassandra username. Blank for passwordless connection. */
    @Value("${dk.dma.ais.store.rest.cassandra.username}")
    private String cassandraUsername;

    /** Cassandra password. Blank for passwordless connection. */
    @Value("${dk.dma.ais.store.rest.cassandra.password}")
    private String cassandraPassword;

    @Bean
    public CassandraConnection provideCassandraConnection() {
        CassandraConnection cassandraConnection = null;
        try {
            if (isBlank(cassandraUsername) || isBlank(cassandraPassword)) {
                LOG.debug("Trying unprotected cassandra connection.");
                cassandraConnection = CassandraConnection.create(cassandraKeyspace, cassandraContactPoints);
            } else {
                LOG.debug("Trying password protected cassandra connection.");
                cassandraConnection = PasswordProtectedCassandraConnection.create(cassandraUsername, cassandraPassword, cassandraKeyspace, cassandraContactPoints);
            }

            cassandraConnection.startAsync();
            cassandraConnection.awaitRunning();

            LOG.info("Connected to Cassandra cluster: \"" + cassandraConnection.getSession().getCluster().getClusterName() + "\" using " + cassandraConnection.getClass().getSimpleName());
        } catch (Exception e) {
            LOG.error("Cannot create Cassandra connection: " + e.getMessage());
            LOG.debug(e.getMessage(), e);
            if (cassandraConnection.state() != Service.State.RUNNING)
                LOG.error("Cassandra error: " + cassandraConnection.failureCause().getMessage());
        }
        return cassandraConnection;
    }

}
