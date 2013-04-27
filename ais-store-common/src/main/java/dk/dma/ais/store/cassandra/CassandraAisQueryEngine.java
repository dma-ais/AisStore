/*
 * Copyright (c) 2008 Kasper Nielsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.dma.ais.store.cassandra;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.joda.time.Interval;

import com.google.common.primitives.Longs;
import com.netflix.astyanax.model.Column;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisQueryEngine;
import dk.dma.db.Query;
import dk.dma.db.cassandra.CassandraCombinedRowQueryMessageSupplier;
import dk.dma.db.cassandra.CassandraRowQueryMessageSupplier;
import dk.dma.db.cassandra.KeySpaceConnection;
import dk.dma.enav.model.geometry.Area;
import dk.dma.enav.model.geometry.grid.Cell;
import dk.dma.enav.model.geometry.grid.Grid;
import dk.dma.enav.util.function.EFunction;

/**
 * 
 * @author Kasper Nielsen
 */
public class CassandraAisQueryEngine implements AisQueryEngine {

    final KeySpaceConnection connection;

    public CassandraAisQueryEngine(KeySpaceConnection connection) {
        this.connection = requireNonNull(connection);
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByArea(Area area, Interval interval) {
        Set<Cell> cells = Grid.CELL1.getCells(area);
        if (cells.size() == 0) {
            Query.emptyQuery();
        }
        List<CassandraRowQueryMessageSupplier<AisPacket, Integer, byte[]>> list = new ArrayList<>();
        for (Cell c : cells) {
            CassandraRowQueryMessageSupplier<AisPacket, Integer, byte[]> s = new CassandraRowQueryMessageSupplier<>(
                    connection, FullSchema.MESSAGES_CELL1, c.getCellId(), new EFunction<Column<byte[]>, AisPacket>() {
                        @Override
                        public AisPacket apply(Column<byte[]> t) throws Exception {
                            return AisPacket.fromByteArray(t.getByteArrayValue());
                        }
                    }, Longs.toByteArray(interval.getStartMillis()), Longs.toByteArray(interval.getEndMillis()));
            list.add(s);
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        System.out.println("Creating a combined query of size " + list.size());
        return new CassandraCombinedRowQueryMessageSupplier<>(list);
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByMMSI(int mmsi, Interval interval) {
        return new CassandraRowQueryMessageSupplier<>(connection, FullSchema.MESSAGES_MMSI, mmsi,
                new EFunction<Column<byte[]>, AisPacket>() {
                    @Override
                    public AisPacket apply(Column<byte[]> t) throws Exception {
                        return AisPacket.fromByteArray(t.getByteArrayValue());
                    }
                }, Longs.toByteArray(interval.getStartMillis()), Longs.toByteArray(interval.getEndMillis()));
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByTime(Interval interval) {
        return null;
    }

    // public void checkAnyOf(TimeUnit unit, TimeUnit... units) {
    // for (TimeUnit r : units) {
    // if (unit == r) {
    // return;
    // }
    // }
    // throw new IllegalArgumentException("Expected one of " + Arrays.toString(units) + ", but was " + unit);
    // }
    //
    // @SuppressWarnings("unused")
    // public Query<Map.Entry<Integer, Integer>> findCells(final int mmsi, final Date start, final Date end)
    // throws Exception {
    // int first = TimeUtil.hoursSinceEpoch(start.getTime());
    // int last = TimeUtil.hoursSinceEpoch(end.getTime());
    //
    // // AllRowsQuery<String, byte[]> allRows = connection.prepareQuery(FullSchema.CELL_OVERVIEW).getAllRows();
    // // //
    // // for (Row<String, byte[]> column : allRows.execute().getResult()) {
    // // if (column.getKey().startsWith("cell")) {
    // // System.out.println(column.getKey());
    // // }
    // // }
    // // RowQuery<String, byte[]> r = connection.prepareQuery(FullSchema.POSITIONS_TMP).getKey("all_376650")
    // // .withColumnRange((byte[]) null, null, false, 100000);
    // // long ss = System.nanoTime();
    // // int count = 0;
    // // for (Column<byte[]> s : r.execute().getResult()) {
    // // count++;
    // // }
    // // System.out.println(count);
    // // System.out.println(System.nanoTime() - ss);
    // // System.out.println(connection.prepareQuery(FullSchema.MMSI).getKey(992761028).execute().getResult().size());
    // //
    // // // .withColumnRange(Ints.toByteArray(first), Ints.toByteArray(last), false, 10000);
    // //
    // // List<AisPacket> result = new ArrayList<>();
    // //
    // // System.out.println(count);
    // return null;
    // }
    //
    // /** {@inheritDoc} */
    // public Query<AisPacket> findForCells(final long cellID, final Date start, final Date end) throws Exception {
    // // return sortByTime(AbstractMultipleResults.forEachEpochMinute(new EFunction<Integer, List<AisPacket>>() {
    // //
    // // @Override
    // // public List<AisPacket> apply(Integer t) throws Exception {
    // // return AisPacket.filterPackets(select(FullSchema.MESSAGES_CELL1_MINUTE, FullSchema.hash(t, cellID)),
    // // start.getTime(), end.getTime());
    // //
    // // }
    // // }, start, end));
    // return null;
    // }
    //
    // //
    // // static AbstractQuery<AisPacket> sortByTime(final AbstractQuery<AisPacket> results) {
    // // return new AbstractQuery<AisPacket>() {
    // //
    // // @Override
    // // protected List<AisPacket> nextBatch() throws Exception {
    // // List<AisPacket> result = results.nextBatch();
    // // if (result != null) {
    // // Collections.sort(result, AisPackets.TIMESTAMP_COMPARATOR);
    // // Collections.reverse(result);
    // // }
    // // return result;
    // // }
    // // };
    // // }
    //
    // // /** {@inheritDoc} */
    // // @Override
    // // public Query<PositionTime> findAllPositions(Date date, TimeUnit timeResolution) throws Exception {
    // // checkAnyOf(timeResolution, TimeUnit.HOURS, TimeUnit.DAYS);
    // // String postfix;
    // // if (timeResolution == TimeUnit.HOURS) {
    // // postfix = "all_hour_" + TimeUtil.hoursSinceEpoch(date.getTime());
    // // } else {
    // // postfix = "all_day_" + TimeUtil.daysSinceEpoch(date.getTime());
    // // }
    // // System.out.println(postfix);
    // // // return columnQuery(postfix, new Function<Column<byte[]>, PositionAndTime>() {
    // // //
    // // // @Override
    // // // public PositionAndTime apply(Column<byte[]> t) {
    // // // long mmsi = Ints.fromByteArray(t.getName());
    // // // System.out.println(mmsi + " " + Position.fromPackedLong(t.getLongValue()));
    // // // return null;
    // // // }
    // // // });
    // // return null;
    // // }
    // List<AisPacket> select(String column, long value) {
    // // IndexQuery<byte[], String> r = connection.prepareQuery(FullSchema.MESSAGES).searchWithIndex().addExpression()
    // // .whereColumn(column).equals().value(value).autoPaginateRows(true)
    // // .withColumnSlice(FullSchema.MESSAGES_MESSAGE);
    // // List<AisPacket> result = new ArrayList<>();
    // // for (Rows<byte[], String> columns = r.execute().getResult(); !columns.isEmpty(); columns = r.execute()
    // // .getResult()) {
    // // for (Row<byte[], String> c : columns) {
    // // ColumnList<String> cl = c.getColumns();
    // // Column<String> message = cl.getColumnByName(FullSchema.MESSAGES_MESSAGE);
    // // AisPacket p = AisPacket.from(message.getStringValue(), -1, null);
    // // result.add(p);
    // // }
    // // }
    // return null;
    // }

}
