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

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.HashViewBuilder;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps.Key2;

/**
 * 
 * @author Jens Tuxen
 * 
 */
public class SourceTimeCount implements HashViewBuilder {
    TypeSafeMapOfMaps<Key2<String, Integer>, Long> data = new TypeSafeMapOfMaps<>();

    private TimeUnit unit;

    @Override
    public void accept(AisPacket aisPacket) {
        try {
            Objects.requireNonNull(aisPacket);
            String source = Objects.requireNonNull(aisPacket.getTags()
                    .getSourceId());
            Long timestamp = aisPacket.getBestTimestamp();

            if (source.equals("")) {
                return;
            }

            if (timestamp > 0) {
                Integer time = AisMatSchema.getTimeBlock(timestamp, unit);
                try {
                    Long value = data.get(TypeSafeMapOfMaps.key(source, time));
                    data.put(TypeSafeMapOfMaps.key(source, time), value + 1);
                } catch (NullPointerException e) {
                    data.put(TypeSafeMapOfMaps.key(source, time), 0L);
                }

            }
        } catch (NullPointerException e1) {
            // TODO Auto-generated catch block
            // e1.printStackTrace();
        }
    }

    public TypeSafeMapOfMaps<Key2<String, Integer>, Long> getData() {
        return data;
    }

    @Override
    public List<RegularStatement> prepare() {
        LinkedList<RegularStatement> list = new LinkedList<>();

        for (Entry<Key2<String, Integer>, Long> e : data) {
            Insert insert = QueryBuilder
                    .insertInto(AisMatSchema.VIEW_KEYSPACE,
                            AisMatSchema.TABLE_CELL1_SOURCE_TIME_COUNT)
                    .value(AisMatSchema.SOURCE_KEY, e.getKey().getK1())
                    .value(AisMatSchema.TIME_KEY, e.getKey().getK2())
                    .value(AisMatSchema.RESULT_KEY, e.getValue());

            list.add(insert);
        }

        return list;
    }

    @Override
    public HashViewBuilder level(TimeUnit unit) {
        this.unit = unit;
        return this;
    }
}
