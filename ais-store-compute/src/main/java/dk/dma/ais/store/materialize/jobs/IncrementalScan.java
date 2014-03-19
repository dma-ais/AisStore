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
package dk.dma.ais.store.materialize.jobs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.HashViewBuilder;
import dk.dma.ais.store.materialize.Scan;
import dk.dma.ais.store.materialize.views.Views;
import dk.dma.db.cassandra.CassandraConnection;

public class IncrementalScan extends Scan {
    private Logger LOG = Logger.getLogger(IncrementalScan.class);

    // This will be a set of timeids, it is sorted
    private TreeSet<Integer> timeIds;
    ArrayList<HashViewBuilder> jobs = new ArrayList<HashViewBuilder>();

    @SuppressWarnings("deprecation")
    @Override
    public void run(Injector arg0) throws Exception {
        // Set up a connection to both AisStore and AisMat
        con = CassandraConnection.create(keySpace, hosts);
        con.start();
        
        jobs.addAll(Views.allForLevel(AisMatSchema.MINUTE_FORMAT));

        viewCluster = Cluster.builder()
                .addContactPoints(viewHosts.toArray(new String[0])).build();
        viewSession = viewCluster.connect(viewKeySpace);
        
        this.init();

        try {
            sw.setStartTime(System.currentTimeMillis());

            timeIds = new TreeSet<Integer>();

            LOG.debug("Retrieving Events");
            RegularStatement select = QueryBuilder.select().from(
                    AisMatSchema.TABLE_STREAM_MONITOR);
            ResultSet s = viewSession.execute(select);

            Iterator<Row> iter = s.iterator();

            while (iter.hasNext()) {
                Row row = iter.next();
                timeIds.add(row.getInt(AisMatSchema.STREAM_TIME_KEY));
                LOG.debug(AisMatSchema.TABLE_STREAM_MONITOR + " Row contains: "
                        + row.getInt(AisMatSchema.STREAM_TIME_KEY));
            }
            LOG.debug("Events Retrieved");
            
            //if (!dummy) {
            //for the purpose of experimentation, the events are never removed
            /*
                LOG.debug("Removing Events from database");
                try {
                    removeEvents();
                } catch (QueryExecutionException e) {
                    //if this fails we lose state
                    reInsertEvents();
                }
                LOG.debug("Removed Events from database");
            }
            */

            for (Integer timeId : timeIds) {
                scan(timeId);
            }
            
            long ms = System.currentTimeMillis() - sw.getStartTime();
            LOG.debug("Total: " + count + " packets,  " + count.get()
                    / ((double) ms / 1000) + " packets/s");

            if (!dummy) {
                postProcess();
            }

            sw.print();
            
        } catch (QueryExecutionException e) {
            reInsertEvents();

        } finally {
            con.stopAsync();
            viewSession.shutdown();
            viewCluster.shutdown();
        }
    }

    @Override
    public void accept(AisPacket t) {
        if (this.count.incrementAndGet() % 1000 == 0) {
            sw.setEndTime(System.currentTimeMillis());
            sw.print();
        }

        for (HashViewBuilder job : jobs) {
            job.accept(t);
        }
    }

    protected void removeEvents() {
        ResultSet rs = viewSession.execute(QueryBuilder
                .delete()
                .all()
                .from(AisMatSchema.TABLE_STREAM_MONITOR)
                .where(QueryBuilder.in(AisMatSchema.TIME_KEY,
                        timeIds.toArray(new Object[0]))));
    }
    
    protected void reInsertEvents() {
        ArrayList<RegularStatement> statements = new ArrayList<>(timeIds.size());
        
        for (Integer timeId: timeIds) {
            statements.add(QueryBuilder.insertInto(AisMatSchema.TABLE_STREAM_MONITOR).value(AisMatSchema.TIME_KEY, timeId));
        }
        
        ResultSet rs = viewSession.execute(QueryBuilder.batch(statements.toArray(new RegularStatement[0])));
    }

    @Override
    protected Iterable<AisPacket> makeRequest() {
        throw new UnsupportedOperationException(
                "Incremental Scanner does not support this, see scan() instead");
    }

    private void scan(Integer timeId) {
        Long start = timeId * 10L * 60L * 1000L;
        Long end = (timeId + 1) * 10L * 60L * 1000L; //exclusive

        Iterable<AisPacket> iterable = con.execute(AisStoreQueryBuilder
                .forTime().setInterval(start, end));

        LOG.debug("STARTING SCAN OF " + timeId);
        sw.setStartTime(System.currentTimeMillis());
        sw.put("currentID", timeId.toString());

        for (AisPacket p : iterable) {
            this.accept(p);

        }

        LOG.debug("ENDING SCAN OF " + timeId);
    }

    public void postProcess() {
        LinkedList<List<RegularStatement>> batches = new LinkedList<>();

        LOG.debug("preparing batches");
        for (HashViewBuilder job : jobs) {
            batches.add(job.prepare());
        }

        LOG.debug("printing all statements");
        for (List<RegularStatement> batch : batches) {
            for (RegularStatement s : batch) {
                LOG.debug(s.getQueryString());
            }
        }
        
        //fail state not handled
        for (List<RegularStatement> batch: batches) {
            viewSession.execute(QueryBuilder.batch(batch.toArray(new RegularStatement[0])));
        }
        

    }

    public static void main(String[] args) throws Exception {
        new IncrementalScan().execute(args);
    }

}
