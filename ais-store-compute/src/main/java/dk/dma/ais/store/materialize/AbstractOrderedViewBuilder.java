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
package dk.dma.ais.store.materialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import dk.dma.ais.packet.AisPacket;

public abstract class AbstractOrderedViewBuilder extends Scan implements
        OrderedViewBuilder {
    Logger LOG = Logger.getLogger(AbstractScanHashViewBuilder.class);

    Integer batchSize = 1000;
    List<RegularStatement> batch = new ArrayList<>(batchSize * 2);

    public void increment(String tableName, Map<String, Object> tuples) {
        Update upd = QueryBuilder.update(tableName);
        upd.setConsistencyLevel(ConsistencyLevel.ANY);

        for (Entry<String, Object> e : tuples.entrySet()) {
            upd.where(QueryBuilder.eq(e.getKey(), e.getValue().toString()));
        }
        upd.with(QueryBuilder.incr(AisMatSchema.RESULT_KEY));

        if (batch.size() % batchSize == 0 && !isDummy()) {
            try {
                viewSession.execute(QueryBuilder.batch(batch
                        .toArray(new RegularStatement[0])));
                batch.clear();
            } catch (QueryExecutionException qe) {
                LOG.error("failed to complete query");
                LOG.error(qe);
            }

        }
    }

    /**
     * Forces the implementation of the Consumer<AisPacket> from super class
     * Scan
     */
    public abstract void accept(AisPacket t);

}
