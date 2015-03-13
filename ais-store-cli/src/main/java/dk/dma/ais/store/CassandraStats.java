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
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Injector;
import dk.dma.ais.packet.AisPacket;
import dk.dma.commons.app.AbstractDaemon;
import dk.dma.commons.management.ManagedResource;
import dk.dma.commons.service.AbstractBatchedStage;
import dk.dma.db.cassandra.CassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMEBLOCK;
import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_TIME;

/**
 * 
 * @author Kasper Nielsen
 */
@ManagedResource
public class CassandraStats extends AbstractDaemon {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(CassandraStats.class);

    @Parameter(names = "-databaseName", description = "Name of the AIS keyspace")
    String databaseName = "aisdata";

    @Parameter(names = "-database", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    @Parameter(names = "-year", description = "Calendar year for which to count AisPackets", required = true)
    int year;

    /** The stage that is responsible for writing the package */
    volatile AbstractBatchedStage<AisPacket> mainStage;

    /** {@inheritDoc} */
    @Override
    protected void runDaemon(Injector injector) throws Exception {
        // Setup keyspace for cassandra
        CassandraConnection con = start(CassandraConnection.create(databaseName, cassandraSeeds));

        printPacketsTimeStats(con);
         /*
        // Start a stage that will write each packet to cassandra
        final AbstractBatchedStage<AisPacket> cassandra = mainStage = start(new DefaultAisStoreWriter(con, batchSize) {
            @Override
            public void onFailure(List<AisPacket> messages, Throwable cause) {
                LOG.error("Could not write batch to cassandra", cause);
                for (AisPacket p : messages) {
                    if (!backupService.getInputQueue().offer(p)) {
                        System.err.println("Could not persist packet!");
                    }
                }
            }
        });*/
    }

    private void printPacketsTimeStats(CassandraConnection conn) {
        final Session session = conn.getSession();

        //for (int year=2005; year< java.time.LocalDate.now().getYear(); year++) {
            final int tb0 = AisStoreSchema.getTimeBlock(TABLE_PACKETS_TIME, Instant.parse(String.format("%4d-01-01T00:00:00.00Z", year)));
            final int tb1 = AisStoreSchema.getTimeBlock(TABLE_PACKETS_TIME, Instant.parse(String.format("%4d-12-31T23:59:59.99Z", year)));
            System.out.println(String.format("Year %d spans %d timeblocks", year, tb1-tb0));

            final int n = tb1-tb0;
            Integer timeblocks[] = new Integer[n];
            for (int i=0; i<n; i++) {
                timeblocks[i] = tb0+i;
            }

            long numPackets = 0;
            final int step = 10;
            for (int i=0; i<=n; i += step) {
                Statement statement = QueryBuilder
                    .select()
                    .countAll()
                    .from(TABLE_PACKETS_TIME.toString())
                    .where(in(COLUMN_TIMEBLOCK.toString(), Arrays.copyOfRange(timeblocks, i, i+step)))
                .setConsistencyLevel(ConsistencyLevel.ONE);
                ResultSet resultSet = session.execute(statement);

                numPackets += resultSet.one().getLong(0);
                printProgress(((float)(i))/n);
            }
            printProgress(100);

            System.out.println(String.format("Counted a total of %l AisPackets in %s.", numPackets, TABLE_PACKETS_TIME.toString()));
        //}
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
