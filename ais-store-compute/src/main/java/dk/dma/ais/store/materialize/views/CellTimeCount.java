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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Map.Entry;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.HashViewBuilder;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps.Key2;
import dk.dma.enav.model.geometry.Position;
/**
 * 
 * @author Jens Tuxen
 *
 */
public class CellTimeCount implements HashViewBuilder {
    TypeSafeMapOfMaps<Key2<Integer, String>, Long> data = new TypeSafeMapOfMaps<>();
    private SimpleDateFormat timeFormatter;

    @Override
    public void accept(AisPacket aisPacket) {
        try {
            Objects.requireNonNull(aisPacket);
            Long timestamp = aisPacket.getBestTimestamp();
            Position p = Objects.requireNonNull(aisPacket.getAisMessage()
                    .getValidPosition());
            Integer cellid = p.getCellInt(1.0);

            if (timestamp > 0) {
                String time = Objects.requireNonNull(timeFormatter
                        .format(new Date(timestamp)));
                try {
                    Long value = data.get(TypeSafeMapOfMaps.key(cellid, time));
                    data.put(TypeSafeMapOfMaps.key(cellid, time), value + 1);
                } catch (Exception e) {
                    data.put(TypeSafeMapOfMaps.key(cellid, time), 0L);
                }

            }
        } catch (AisMessageException | SixbitException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

    @Override
    public List<RegularStatement> prepare() {
        LinkedList<RegularStatement> list = new LinkedList<>();
        for (Entry<Key2<Integer, String>, Long> e : data) {
            Update upd = QueryBuilder
                    .update(AisMatSchema.TABLE_CELL1_TIME_COUNT);
            upd.setConsistencyLevel(ConsistencyLevel.ONE);
            upd.where(QueryBuilder.eq(AisMatSchema.CELL1_KEY, e.getKey()
                    .getK1()));
            upd.where(QueryBuilder
                    .eq(AisMatSchema.TIME_KEY, e.getKey().getK2()));
            upd.with(QueryBuilder.set(AisMatSchema.RESULT_KEY, e.getValue()));
            list.add(upd);

        }
        return list;
    }

    @Override
    public HashViewBuilder level(SimpleDateFormat timeFormatter) {
        this.timeFormatter = timeFormatter;
        return this;
    }
}
