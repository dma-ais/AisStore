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
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.inject.Injector;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.TimeScan;

/**
 * @author Jens Tuxen
 * 
 */
public class CountMMSIAis extends TimeScan {
    private static Logger LOG = Logger.getLogger(CountMMSIAis.class);
    
    @Parameter(names = "-timeFormatter", description = "time resolution")
    String timeformat = AisMatSchema.HOUR_FORMAT;
    
    SimpleDateFormat timeFormatter;
    
    
    private Integer batchSize = 10000;
    Map<Integer, Map<String, Long>> data = new WeakHashMap<Integer, Map<String, Long>>(100000);
    
    
    @Parameter(names = "-csv", required = false, description = "absolute path to csv result")
    protected String csvString = "CountMMSIAis.csv";
    
    
    public void run(Injector arg0) throws Exception {
        PrintWriter csv = new PrintWriter(new BufferedOutputStream(new FileOutputStream(
                csvString)));
        timeFormatter = new SimpleDateFormat(timeformat);
        try {
            super.run(arg0);
            csv.print(this.toCSV());
        } finally {
            csv.close();
        }
    }


    protected void buildView() {
        LOG.debug("Starting view building");
        ArrayList<Statement> statements = new ArrayList<Statement>(batchSize + 1);
        long c = 0;
        long start = System.currentTimeMillis();
        for (Entry<Integer, Map<String, Long>> entry: data.entrySet()) {
            Integer mmsi = entry.getKey();
            HashMap<String, Long> map = (HashMap<String, Long>)entry.getValue();
            for (Entry<String, Long> dayEntry: map.entrySet()) {
                c++;
                Update upd = QueryBuilder.update(AisMatSchema.TABLE_MMSI_TIME_COUNT);
                upd.setConsistencyLevel(ConsistencyLevel.ANY);
                upd.where(QueryBuilder.eq(AisMatSchema.MMSI_KEY, mmsi.intValue()));
                upd.where(QueryBuilder.eq(AisMatSchema.TIME_KEY,dayEntry.getKey().toString()));
                upd.with(QueryBuilder.set(AisMatSchema.VALUE, dayEntry.getValue()));              
                statements.add(upd);
                if (c % batchSize == 0) {
                    
                    long ms = System.currentTimeMillis() - start;
                    LOG.debug("count: " + c + " inserts,  " + c
                            / ((double) ms / 1000) + " inserts/s");
                    

                    ResultSetFuture futures = viewSession.executeAsync(QueryBuilder.batch(statements.toArray(new Statement[0])));
                    while(!futures.isDone()) {
                        System.out.println(futures.isDone());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        
                    }
                    statements.clear();
                }
            }
        }

    }

    @Override
    public void accept(AisPacket aisPacket) {
        if (aisPacket == null) {
            return;
        }
        
        try {
            
            Integer key = aisPacket.getAisMessage().getUserId();
            Long timestamp = aisPacket.getBestTimestamp();

            if (timestamp > 0) {
                String day = timeFormatter.format(new Date(timestamp));
                
                if (data.containsKey(key)) {
                    if (data.get(key).containsKey(day)) {
                        data.get(key).put(day,
                                data.get(key).get(day) + 1);
                    } else {
                        data.get(key).put(day, 1L);
                    }

                    
                } else {
                    data.put(key, new WeakHashMap<String,Long>(24));
                    data.get(key).put(day, 1L);
                }
                
            }

        } catch (AisMessageException | SixbitException e) {
            //e.printStackTrace();
        }
        
    }
    
    

    
    
    public static void main(String[] args) throws Exception {
        new CountMMSIAis().execute(args);
    }



}
