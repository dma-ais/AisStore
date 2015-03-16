/* Copyright (c) 2011 Danish Maritime Authority.
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
package dk.dma.ais.store;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.inject.Injector;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.management.ManagedResource;
import dk.dma.db.cassandra.CassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMEBLOCK;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMESTAMP;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_TIME;
import static java.lang.Integer.min;

/**
 * 
 * @author Thomas Borg Salling
 */
@ManagedResource
public class CassandraStats extends AbstractCommandLineTool {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(CassandraStats.class);

    @Parameter(names = "-databaseName", description = "Name of the AIS keyspace")
    String databaseName = "aisdata";

    @Parameter(names = "-database", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    @Parameter(names = "-from", description = "The instant to count from (format \"2015-01-01T00:00:00.000Z\") (inclusive)", required = true)
    String from;

    @Parameter(names = "-to", description = "The instant to count to (format \"2015-02-01T00:00:00.000Z\") (exclusive)", required = true)
    String to;

    @Parameter(names = "-tb", description = "No. of timeblocks per select query")
    int tbPerSelect = 10;

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        // Setup keyspace for cassandra
        CassandraConnection con = start(CassandraConnection.create(databaseName, cassandraSeeds));
        printPacketsTimeStats(con);
    }

    private void printPacketsTimeStats(CassandraConnection conn) {
        final Session session = conn.getSession();

        final Instant ts0 = Instant.parse(from);
        final Instant ts1 = Instant.parse(to);

        final int tb0 = AisStoreSchema.timeBlock(TABLE_PACKETS_TIME, ts0);
        final int tb1 = AisStoreSchema.timeBlock(TABLE_PACKETS_TIME, ts1);

        LOG.debug(String.format("Duration from %s to %s spans %d timeblocks", from, to, tb1 - tb0));

        final int n = tb1-tb0+1; // no. of timeblocks to include in query
        Integer timeblocks[] = new Integer[n];
        for (int i=0; i<n; i++) {
            timeblocks[i] = tb0+i;
        }

        long numPackets = 0;
        final int step = tbPerSelect;
        for (int i=0; i<n; i += step) {
            Integer[] queryTimeBlocks = Arrays.copyOfRange(timeblocks, i, min(i + step, n));

            Statement statement = QueryBuilder
                .select()
                .countAll()
                .from(TABLE_PACKETS_TIME.toString())
                .where(in(COLUMN_TIMEBLOCK.toString(), queryTimeBlocks));

            if (i == 0)
                ((Select.Where) statement).and(gte(COLUMN_TIMESTAMP.toString(), ts0.toEpochMilli()));

            if (i >= n-step)
                ((Select.Where) statement).and(lt(COLUMN_TIMESTAMP.toString(), ts1.toEpochMilli()));

            statement.setConsistencyLevel(ConsistencyLevel.ALL);

            LOG.debug(((Select.Where) statement).getQueryString());

            ResultSetFuture future = session.executeAsync(statement);
            ResultSet resultSet = future.getUninterruptibly();

            final long count = resultSet.one().getLong(0);

            numPackets += count;

            System.out.println("Timeblocks " + queryTimeBlocks[0] + ".." + queryTimeBlocks[queryTimeBlocks.length-1] + ": " + count);

            printProgress(((float) (i))/n);
        }
        printProgress(100);

        System.out.println(String.format("Counted a total of %d AisPackets in %s.", numPackets, TABLE_PACKETS_TIME.toString()));
    }

    private static void printProgress(float pct) {
        System.out.print(String.format("%5.1f %% done counting AisPackets in %s\r", pct*100f, TABLE_PACKETS_TIME.toString()));
    }

    public static void main(String[] args) throws Exception {
        // args = AisReaders.getDefaultSources();
        if (args.length == 0) {
            System.err.println("Must specify at least 1 source (sourceName=host:port,host:port sourceName=host:port)");
            System.exit(1);
        }
        new CassandraStats().execute(args);
    }
}
