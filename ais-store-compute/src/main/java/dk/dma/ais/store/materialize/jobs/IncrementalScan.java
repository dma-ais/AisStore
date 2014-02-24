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

import java.awt.geom.Area;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreSchema;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.Scan;

public class IncrementalScan extends Scan {
    private Logger LOG = Logger.getLogger(IncrementalScan.class);

    // This will be a sorted list of timeids, it is pre-sorted by AisStore
    LinkedList<Integer> timeIds;
    
    
    ArrayList<Scan> jobs = new ArrayList<Scan>();

    @Override
    public void run(Injector arg0) throws Exception {
        super.run(arg0);
        try {
            setStartTime(System.currentTimeMillis());

            timeIds = new LinkedList<>();

            RegularStatement select = QueryBuilder.select().from(
                    AisMatSchema.TABLE_STREAM_MONITOR);
            ResultSet s = viewSession.execute(select);
            Iterator<Row> iter = s.iterator();

            while (iter.hasNext()) {
                Row row = iter.next();

                timeIds.add(row.getInt(AisMatSchema.TIME_KEY));

                if (count.get() % batchSize == 0) {
                    long ms = System.currentTimeMillis() - startTime;
                    System.out
                            .println(count.get() + " packets,  " + count.get()
                                    / ((double) ms / 1000) + " packets/s");
                }
            }
            
            

            long ms = System.currentTimeMillis() - startTime;
            System.out.println("Total: " + count + " packets,  " + count.get()
                    / ((double) ms / 1000) + " packets/s");

            if (!dummy) {
                postProcess();
            }

            setEndTime(System.currentTimeMillis());

        } finally {
            con.stop();
            viewSession.shutdown();
        }
    }

    @Override
    public void accept(AisPacket t) {
        
    }

    @Override
    protected Iterable<AisPacket> makeRequest() {
        Integer minimum = timeIds.getFirst();
        Integer maximum = timeIds.getLast();

        RegularStatement select = QueryBuilder
                .select(AisStoreSchema.COLUMN_AISDATA)
                .from(AisStoreSchema.TABLE_TIME)
                .where(QueryBuilder.lt(AisStoreSchema.COLUMN_TIMEHASH, minimum))
                .and(QueryBuilder.gt(AisStoreSchema.COLUMN_TIMEHASH, maximum));

        LOG.debug(select);
        
        return null;

    }

}
