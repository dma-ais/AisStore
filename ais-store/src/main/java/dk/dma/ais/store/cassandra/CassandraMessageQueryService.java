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

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.primitives.Longs;
import com.netflix.astyanax.model.Column;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.query.MessageQueryService;
import dk.dma.ais.store.util.CellPositionMmsi;
import dk.dma.ais.store.util.CellResolution;
import dk.dma.ais.store.util.PositionAndTime;
import dk.dma.ais.store.util.TimeUtil;
import dk.dma.app.cassandra.CassandraRowQueryMessageSupplier;
import dk.dma.app.cassandra.KeySpaceConnection;
import dk.dma.app.cassandra.Query;
import dk.dma.app.util.function.EFunction;
import dk.dma.enav.model.geometry.Area;

/**
 * 
 * @author Kasper Nielsen
 */
public class CassandraMessageQueryService implements MessageQueryService {

    final KeySpaceConnection connection;

    public CassandraMessageQueryService(KeySpaceConnection connection) {
        this.connection = requireNonNull(connection);
    }

    public void checkAnyOf(TimeUnit unit, TimeUnit... units) {
        for (TimeUnit r : units) {
            if (unit == r) {
                return;
            }
        }
        throw new IllegalArgumentException("Expected one of " + Arrays.toString(units) + ", but was " + unit);
    }

    /** {@inheritDoc} */
    @Override
    public Query<PositionAndTime> findAllPositions(Date date, TimeUnit timeResolution) throws Exception {
        checkAnyOf(timeResolution, TimeUnit.HOURS, TimeUnit.DAYS);
        String postfix;
        if (timeResolution == TimeUnit.HOURS) {
            postfix = "all_hour_" + TimeUtil.hoursSinceEpoch(date.getTime());
        } else {
            postfix = "all_day_" + TimeUtil.daysSinceEpoch(date.getTime());
        }
        System.out.println(postfix);
        // return columnQuery(postfix, new Function<Column<byte[]>, PositionAndTime>() {
        //
        // @Override
        // public PositionAndTime apply(Column<byte[]> t) {
        // long mmsi = Ints.fromByteArray(t.getName());
        // System.out.println(mmsi + " " + Position.fromPackedLong(t.getLongValue()));
        // return null;
        // }
        // });
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByMMSI(final Date start, final Date end, int mmsi) {
        return findByMMSI(start.getTime(), end.getTime(), mmsi);
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByMMSI(Interval interval, int mmsi) {
        return findByMMSI(interval == null ? 1 : interval.getStartMillis(), interval == null ? Long.MAX_VALUE
                : interval.getEndMillis(), mmsi);
    }

    /** {@inheritDoc} */
    Query<AisPacket> findByMMSI(final long startMillies, final long endMillies, final int mmsi) {
        return new CassandraRowQueryMessageSupplier<>(connection, FullSchema.MESSAGES_MMSI, mmsi,
                new EFunction<Column<byte[]>, AisPacket>() {
                    @Override
                    public AisPacket apply(Column<byte[]> t) throws Exception {
                        return AisPacket.fromByteArray(t.getByteArrayValue());
                    }
                }, Longs.toByteArray(startMillies), Longs.toByteArray(endMillies));
    }

    /** {@inheritDoc} */
    public Query<AisPacket> findByMMSI(String isoXXInterval, int mmsi) {
        if (!isoXXInterval.contains("/")) {
            isoXXInterval += "/" + DateTime.now();
        }
        Interval parse = Interval.parse(isoXXInterval);
        return findByMMSI(parse.getStartMillis(), parse.getEndMillis(), mmsi);
    }

    /** {@inheritDoc} */
    Query<AisPacket> findByShape(Area shape, final long startMillies, final long endMillies) {
        return new CassandraRowQueryMessageSupplier<>(connection, FullSchema.MESSAGES_CELL1, 20172,
                new EFunction<Column<byte[]>, AisPacket>() {
                    @Override
                    public AisPacket apply(Column<byte[]> t) throws Exception {
                        return AisPacket.fromByteArray(t.getByteArrayValue());
                    }
                }, Longs.toByteArray(startMillies), Longs.toByteArray(endMillies));
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByShape(Area shape, Date start, Date end) {
        return findByShape(shape, start.getTime(), end.getTime());
        // return new CassandraRowQueryMessageSupplier<>(connection, FullSchema.MESSAGES_TIME, 2260481,
        // new EFunction<Column<byte[]>, AisPacket>() {
        // @Override
        // public AisPacket apply(Column<byte[]> t) throws Exception {
        // return AisPacket.fromByteArray(t.getByteArrayValue());
        // }
        // }, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByShape(Area shape, long timeback, TimeUnit unit) {
        Date now = new Date();
        return findByShape(shape, TimeUtil.substract(now, timeback, unit), now);
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByTime(Date start, Date end) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByTime(Interval interval) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<AisPacket> findByTime(String interval) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<PositionAndTime> findCells(Date date, TimeUnit timeResolution) throws Exception {
        return null;
    }

    @SuppressWarnings("unused")
    public Query<Map.Entry<Integer, Integer>> findCells(final int mmsi, final Date start, final Date end)
            throws Exception {
        int first = TimeUtil.hoursSinceEpoch(start.getTime());
        int last = TimeUtil.hoursSinceEpoch(end.getTime());

        // AllRowsQuery<String, byte[]> allRows = connection.prepareQuery(FullSchema.CELL_OVERVIEW).getAllRows();
        // //
        // for (Row<String, byte[]> column : allRows.execute().getResult()) {
        // if (column.getKey().startsWith("cell")) {
        // System.out.println(column.getKey());
        // }
        // }
        // RowQuery<String, byte[]> r = connection.prepareQuery(FullSchema.POSITIONS_TMP).getKey("all_376650")
        // .withColumnRange((byte[]) null, null, false, 100000);
        // long ss = System.nanoTime();
        // int count = 0;
        // for (Column<byte[]> s : r.execute().getResult()) {
        // count++;
        // }
        // System.out.println(count);
        // System.out.println(System.nanoTime() - ss);
        // System.out.println(connection.prepareQuery(FullSchema.MMSI).getKey(992761028).execute().getResult().size());
        //
        // // .withColumnRange(Ints.toByteArray(first), Ints.toByteArray(last), false, 10000);
        //
        // List<AisPacket> result = new ArrayList<>();
        //
        // System.out.println(count);
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<CellPositionMmsi> findCells(int mmsi, Date start, Date stop, CellResolution cellResolution)
            throws Exception {
        return null;
    }

    /** {@inheritDoc} */
    public Query<Entry<Integer, Integer>> findCells(int mmsi, long timeback, TimeUnit unit) throws Exception {
        Date now = new Date();
        return findCells(mmsi, TimeUtil.substract(now, timeback, unit), now);
    }

    /** {@inheritDoc} */
    @Override
    public Query<CellPositionMmsi> findCells(int mmsi, long timeback, TimeUnit unit, CellResolution resolution)
            throws Exception {
        return null;
    }

    /** {@inheritDoc} */
    public Query<AisPacket> findForCells(final long cellID, final Date start, final Date end) throws Exception {
        // return sortByTime(AbstractMultipleResults.forEachEpochMinute(new EFunction<Integer, List<AisPacket>>() {
        //
        // @Override
        // public List<AisPacket> apply(Integer t) throws Exception {
        // return AisPacket.filterPackets(select(FullSchema.MESSAGES_CELL1_MINUTE, FullSchema.hash(t, cellID)),
        // start.getTime(), end.getTime());
        //
        // }
        // }, start, end));
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<CellPositionMmsi> findInCell(int cellId, CellResolution resolution, Date start, Date end)
            throws Exception {
        return null;
    }

    //
    // static AbstractQuery<AisPacket> sortByTime(final AbstractQuery<AisPacket> results) {
    // return new AbstractQuery<AisPacket>() {
    //
    // @Override
    // protected List<AisPacket> nextBatch() throws Exception {
    // List<AisPacket> result = results.nextBatch();
    // if (result != null) {
    // Collections.sort(result, AisPackets.TIMESTAMP_COMPARATOR);
    // Collections.reverse(result);
    // }
    // return result;
    // }
    // };
    // }

    /** {@inheritDoc} */
    @Override
    public Query<CellPositionMmsi> findInCell(int cellId, CellResolution resolution, long timeback, TimeUnit unit)
            throws Exception {
        return null;
    }

    /** {@inheritDoc} */
    public Query<AisPacket> findLastForCells(long cell, long timeback, TimeUnit unit) throws Exception {
        Date now = new Date();
        return findForCells(cell, TimeUtil.substract(now, timeback, unit), now);
    }

    /** {@inheritDoc} */
    @Override
    public Query<PositionAndTime> findPositions(int mmsi, Date date, CellResolution cellResolution,
            TimeUnit timeResolution) throws Exception {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Query<PositionAndTime> findPositions(int mmsi, long timeback, TimeUnit unit, CellResolution resolution,
            TimeUnit timeResolution) throws Exception {
        return null;
    }

    List<AisPacket> select(String column, long value) {
        // IndexQuery<byte[], String> r = connection.prepareQuery(FullSchema.MESSAGES).searchWithIndex().addExpression()
        // .whereColumn(column).equals().value(value).autoPaginateRows(true)
        // .withColumnSlice(FullSchema.MESSAGES_MESSAGE);
        // List<AisPacket> result = new ArrayList<>();
        // for (Rows<byte[], String> columns = r.execute().getResult(); !columns.isEmpty(); columns = r.execute()
        // .getResult()) {
        // for (Row<byte[], String> c : columns) {
        // ColumnList<String> cl = c.getColumns();
        // Column<String> message = cl.getColumnByName(FullSchema.MESSAGES_MESSAGE);
        // AisPacket p = AisPacket.from(message.getStringValue(), -1, null);
        // result.add(p);
        // }
        // }
        return null;
    }
}
