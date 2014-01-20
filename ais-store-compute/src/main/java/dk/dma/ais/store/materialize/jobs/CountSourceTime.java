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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketTags;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.TimeScan;

/**
 * @author Jens Tuxen
 * 
 */
public class CountSourceTime extends TimeScan {
    private static Logger LOG = Logger.getLogger(CountSourceTime.class);

    @Parameter(names = "-timeFormatter", description = "time resolution")
    String timeformat = AisMatSchema.MINUTE_FORMAT;

    SimpleDateFormat timeFormatter;
    
    Integer batchSize = 1000;
    List<Statement> batch = new ArrayList<>(batchSize*2);
    
    public void run(Injector arg0) throws Exception {
        timeFormatter = new SimpleDateFormat(timeformat);

        super.run(arg0);

    }

    /**
     * 
     * @param sourceid
     * @param timekey
     */
    protected synchronized void increment(String sourceid, String timekey) {
        Update upd = QueryBuilder.update(AisMatSchema.TABLE_SOURCE_TIME_COUNT);
        upd.setConsistencyLevel(ConsistencyLevel.ANY);
        upd.where(QueryBuilder.eq(AisMatSchema.SOURCE_KEY, sourceid));
        upd.where(QueryBuilder.eq(AisMatSchema.TIME_KEY, timekey));
        upd.with(QueryBuilder.incr(AisMatSchema.COUNTER_KEY));
        
        if (batch.size() % batchSize == 0) {
            try {
                viewSession.execute(QueryBuilder.batch(batch.toArray(new Statement[0])));
                batch.clear();
            } catch (QueryExecutionException qe) {
                LOG.error("failed to complete query");
                LOG.error(qe);
            }
            
         
        }
    }

    @Override
    public void accept(AisPacket aisPacket) {
        try {
            Objects.requireNonNull(aisPacket);
            AisPacketTags t = Objects.requireNonNull(aisPacket.getTags());
            String sourceid = Objects.requireNonNull(t.getSourceId());
            Long timestamp = aisPacket.getBestTimestamp();

            if (sourceid.length() > 0 && timestamp > 0) {
                String time = timeFormatter.format(new Date(timestamp));
                increment(sourceid, time);
                
            }
            
            
            
        } catch (NullPointerException e) {
        }

    }

    @Override
    protected void buildView() {
        // not used currently for this view

    }

    public static void main(String[] args) throws Exception {
        new CountSourceTime().execute(args);
    }
}
