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
package dk.dma.ais.store.materialize.cli;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.inject.Injector;

import dk.dma.ais.store.AisStoreSchema;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.db.cassandra.CassandraConnection;


public class AbstractViewCommandLineTool extends
        AbstractCommandLineTool {
    
    @Parameter(names = "-hosts", required = true, description = "hosts in the format host:port,host:port")
    protected List<String> hosts;
    
    @Parameter(names = "-viewHosts", required = false, description = "hosts in the format host:port,host:port")
    protected List<String> viewHosts;
    
    
    @Parameter(names = "-viewKeyspace", required = false, description = "keyspace for the views")
    protected String viewKeySpace = AisMatSchema.VIEW_KEYSPACE;

    @Parameter(names = "-keySpace", description = "keyspace for the asdata")
    protected String keySpace = AisStoreSchema.COLUMN_AISDATA;
    
    protected CassandraConnection con;
    protected Cluster viewCluster;
    protected Session viewSession;
    
    protected AtomicInteger count = new AtomicInteger();
    
    @SuppressWarnings("deprecation")
    protected  void run(Injector arg0) throws Exception {
        // Set up a connection to both AisStore and AisMat
        con = CassandraConnection.create(keySpace, hosts);
        con.start();
        
        viewCluster = Cluster.builder().addContactPoints(viewHosts.toArray(new String[0])).build();
        viewSession = viewCluster.connect(viewKeySpace);
    }
    
    public Integer getCountValue() {
        return count.get();
    }
    

}
