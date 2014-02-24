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

/**
 * @author Jens Tuxen
 */
package dk.dma.ais.store.materialize.jobs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.inject.Injector;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.ais.store.materialize.AbstractScanHashViewBuilder;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps.Key2;
import dk.dma.ais.store.views.MMSITimeCount;
/**
 * @author Jens Tuxen
 * 
 */
public class CountMMSIAis extends AbstractScanHashViewBuilder {
    private static Logger LOG = Logger.getLogger(AbstractScanHashViewBuilder.class);
    
    @Parameter(names = "-timeFormatter", description = "time resolution")
    String timeformat = AisMatSchema.HOUR_FORMAT;
    
    private Integer batchSize = 10000;
    
    @Parameter(names = "-csv", required = false, description = "absolute path to csv result")
    protected String csvString = "CountMMSIAis.csv";
    
    
    MMSITimeCount view = new MMSITimeCount(new SimpleDateFormat(timeformat));
    
    public void run(Injector arg0) throws Exception {
        PrintWriter csv = new PrintWriter(new BufferedOutputStream(new FileOutputStream(
                csvString)));

        try {
            super.run(arg0);
            csv.print(this.toCSV());
        } finally {
            csv.close();
        }
    }


    public static void main(String[] args) throws Exception {
        new CountMMSIAis().execute(args);
    }


    @Override
    protected Iterable<AisPacket> makeRequest() {
        return con.execute(AisStoreQueryBuilder.forTime().setInterval(start.getTime(),stop.getTime()));
    }



    @Override
    public void postProcess() {
        LOG.debug("Starting view building");
        ArrayList<RegularStatement> statements = new ArrayList<RegularStatement>(batchSize + 1);
        long c = 0;
        long start = System.currentTimeMillis();
        
        
        for (Entry<Key2<Long, String>, Long> e : view.getData()) {
            c++;
            
            Update upd = QueryBuilder.update(AisMatSchema.TABLE_MMSI_TIME_COUNT);
            upd.setConsistencyLevel(ConsistencyLevel.ONE);
            upd.where(QueryBuilder.eq(AisMatSchema.MMSI_KEY, e.getKey().getK1()));
            upd.where(QueryBuilder.eq(AisMatSchema.TIME_KEY, e.getKey().getK2()));
            upd.with(QueryBuilder.set(AisMatSchema.RESULT_KEY, e.getValue()));              
            statements.add(upd);

            if (c % batchSize == 0) {
                c = 0;
                long ms = System.currentTimeMillis() - start;
                LOG.debug("count: " + c + " inserts,  " + c
                        / ((double) ms / 1000) + " inserts/s");
                
                try {
                    
                    viewSession.execute(QueryBuilder.batch(statements
                            .toArray(new RegularStatement[0])));
                    statements.clear();
                } catch (QueryExecutionException qe) {
                    LOG.error("failed to complete query");
                    LOG.error(qe);
                    this.sleep(5000);
                }
                
            }
            
        }

    }
                
     

    private void sleep(Integer ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }


    @Override
    public void accept(AisPacket t) {
        // TODO Auto-generated method stub
        
    }


}
