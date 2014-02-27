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
package dk.dma.ais.store.materialize.views;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Map.Entry;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.HashViewBuilder;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps.Key2;

public class MMSITimeCount implements HashViewBuilder {
    TypeSafeMapOfMaps<Key2<Long, String>, Long> data = new TypeSafeMapOfMaps<>();
    private SimpleDateFormat timeFormatter;

    public MMSITimeCount(SimpleDateFormat timeFormatter) {
        this.timeFormatter = timeFormatter;

    }

    @Override
    public void accept(AisPacket aisPacket) {
        try {
            Objects.requireNonNull(aisPacket);
            Long mmsi = (long) Objects.requireNonNull(aisPacket.getAisMessage()
                    .getUserId());
            Long timestamp = aisPacket.getBestTimestamp();

            if (timestamp > 0) {
                String time = Objects.requireNonNull(timeFormatter
                        .format(new Date(timestamp)));
                try {
                    Long value = data.get(TypeSafeMapOfMaps.key(mmsi, time));
                    data.put(TypeSafeMapOfMaps.key(mmsi, time), value + 1);
                } catch (Exception e) {
                    data.put(TypeSafeMapOfMaps.key(mmsi, time), 0L);
                }

            }

        } catch (AisMessageException | SixbitException e) {
            // e.printStackTrace();
        }
    }

    public TypeSafeMapOfMaps<Key2<Long, String>, Long> getData() {
        return data;
    }

    @Override
    public List<RegularStatement> prepare() {
        LinkedList<RegularStatement> list = new LinkedList<>();
        for (Entry<Key2<Long, String>, Long> e : data) {
            Update upd = QueryBuilder
                    .update(AisMatSchema.TABLE_MMSI_TIME_COUNT);
            upd.setConsistencyLevel(ConsistencyLevel.ONE);
            upd.where(QueryBuilder
                    .eq(AisMatSchema.MMSI_KEY, e.getKey().getK1()));
            upd.where(QueryBuilder
                    .eq(AisMatSchema.TIME_KEY, e.getKey().getK2()));
            upd.with(QueryBuilder.set(AisMatSchema.RESULT_KEY, e.getValue()));
            list.add(upd);

        }
        return list;
    }
}
