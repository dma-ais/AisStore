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
package dk.dma.ais.store.compute.jobs;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.commons.util.concurrent.ForkJoinUtil;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.util.function.Consumer;

/**
 * @author jtj
 * 
 */
public class CountMMSIAis implements ScanJob {
    CassandraConnection con = CassandraConnection.create("aisdata",
            "192.168.56.101");

    Cluster cluster = Cluster.builder().addContactPoint("192.168.56.101")
            .build();

    Session dssSession = cluster.connect("dss");
    Map<Integer, Map<String, Long>> tempResults = new ConcurrentHashMap<Integer, Map<String, Long>>();
    
    final SimpleDateFormat levelFormatter = new SimpleDateFormat("yyyyMMdd");

    long check = 0;

    int count = 0;
    int batchSize = 10000;
    double previousBatch;

    /**
     * 
     */
    public CountMMSIAis() {
        con.start();
    }

    public void run() {
        try {
            check = System.currentTimeMillis();
            Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder
                    .forTime().setInterval(
                            new Date().getTime() - (1000 * 60 * 60 * 24 * 20),
                            new Date().getTime()));            

            long start = System.currentTimeMillis();
            
            AisPacket[] apArray = new AisPacket[500];
            
            final CountMMSIAis THIS = this;
            for (AisPacket p : iter) {
                
                if (p != null) {
                    count++;
                    int pos = count % 500;
                    apArray[pos] = p;
                    
                    if (pos == 499) {
                        ForkJoinUtil.forEach(apArray,new Consumer<AisPacket>() {

                            @Override
                            public void accept(AisPacket arg0) {
                                THIS.process(arg0);   
                            }
                        });
                    }
                    
                    
                    //this.process(p);
                    
                }

                if (count % batchSize == 0) {
                    long ms = System.currentTimeMillis() - start;
                    System.out.println(count + " packets,  " + count
                            / ((double) ms / 1000) + " packets/s");
                }
            }
            long ms = System.currentTimeMillis() - start;
            System.out.println("Total: " + count + " packets,  " + count
                    / ((double) ms / 1000) + " packets/s");
        } finally {
            con.stop();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * dk.dma.ais.store.compute.jobs.IPacketJob#process(dk.dma.ais.packet.AisPacket
     * )
     */
    @Override
    public void process(AisPacket aisPacket) {
        AisMessage aisMessage = null;
        try {
            aisMessage = aisPacket.getAisMessage();
            Integer key = aisMessage.getUserId();
            Long timestamp = aisPacket.getBestTimestamp();
            String day = levelFormatter.format(new Date(timestamp));

            if (timestamp != -1L) {
                if (tempResults.containsKey(key)) {
                    if (tempResults.get(key).containsKey(day)) {
                        tempResults.get(key).put(day,
                                tempResults.get(key).get(day) + 1);
                    } else {
                        tempResults.get(key).put(day, 1L);
                    }

                }
                ConcurrentHashMap<String, Long> value = new ConcurrentHashMap<String, Long>();
                value.put(day, 1L);
                tempResults.put(key, value);
            }

        } catch (AisMessageException | SixbitException e) {
            // e.printStackTrace();
        }

    }

    public void update() {
        ArrayList<Statement> statements = new ArrayList<Statement>(batchSize + 1);
        long c = 0;
        long start = System.currentTimeMillis();
        for (Entry<Integer, Map<String, Long>> entry: tempResults.entrySet()) {
            Integer mmsi = entry.getKey();
            HashMap<String, Long> map = (HashMap<String, Long>)entry.getValue();
            for (Entry<String, Long> dayEntry: map.entrySet()) {
                c++;
                Update upd = QueryBuilder.update("dss_mmsi_count");
                upd.setConsistencyLevel(ConsistencyLevel.ANY);
                upd.with(QueryBuilder.set("result", dayEntry.getValue()));
                
                upd.where(QueryBuilder.eq("check", check));
                upd.where(QueryBuilder.eq("mmsi", mmsi.intValue()));
                upd.where(QueryBuilder.eq("level",dayEntry.getKey().toString()));
                              
                statements.add(upd);
                if (c % batchSize == 0) {
                    
                    long ms = System.currentTimeMillis() - start;
                    System.out.println("count: " + c + " inserts,  " + c
                            / ((double) ms / 1000) + " inserts/s");
                    

                    ResultSetFuture futures = dssSession.executeAsync(QueryBuilder.batch(statements.toArray(new Statement[statements.size()])));
                    while(futures.isDone() == false) {
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

        System.out.println("DONE");
        dssSession.shutdown();
    }

    public static void main(String[] args) {
        CountMMSIAis cma = new CountMMSIAis();
        cma.run();
        cma.update();

    }

}
