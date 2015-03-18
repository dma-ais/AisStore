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
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.inject.Injector;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.store.AisStoreSchema.Table;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.management.ManagedResource;
import dk.dma.db.cassandra.CassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_AISDATA;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMEBLOCK;
import static dk.dma.ais.store.AisStoreSchema.Column.COLUMN_TIMESTAMP;
import static dk.dma.ais.store.AisStoreSchema.timeBlock;

/**
 * @author Thomas Borg Salling
 *
 * TODO: This class is naively implemented and still experimental. It is intended to
 * assist in pin-pointing AisPackets which are in data files, but not in Cassandra.
 */
@ManagedResource
public class FileDiff extends AbstractCommandLineTool {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(FileDiff.class);

    @Parameter(names = "-databaseName", description = "Name of the AIS keyspace")
    String databaseName = "aisdata";

    @Parameter(names = "-database", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    @Parameter(names = "-tag", description = "Overwrite or add the tag")
    String tag;

    @Parameter(names = {"-path", "-input", "-i"}, description = "Path to directory with files to read for diff", required = true)
    String path;

    @Parameter(names = "-recursive", description = "recursive directory reader")
    boolean recursive = true;

    @Parameter(names = "-glob", description = "pattern for files to read (default *)")
    String glob = "*";

    @Parameter(names = {"-table", "-t"}, description = "The table in which to lookup data in Cassandra")
    String tableName = "packets_time";

    @Parameter(names = {"-cl"}, description = "Consistency level for Cassandra queries")
    String consistencyLevel = "ONE";

    @Parameter(names = {"-threads"}, description = "No. of concurrent queries to Cassandra")
    int concurrencyLevel = 1;

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        // Setup keyspace for cassandra
        CassandraConnection con = start(CassandraConnection.create(databaseName, cassandraSeeds));
        diffPackets(con);
    }

    private void diffPackets(CassandraConnection conn) throws IOException, InterruptedException {
        final Session session = conn.getSession();

        final Table table = Table.valueOf("TABLE_" + tableName.toUpperCase());
        final ConsistencyLevel cl = ConsistencyLevel.valueOf(consistencyLevel.trim().toUpperCase()); //toConsistencyLevel(consistencyLevel);
        final ExecutorService executorService =
            new ThreadPoolExecutor(
                concurrencyLevel, concurrencyLevel, 5L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1024, true), new ThreadPoolExecutor.CallerRunsPolicy()
            );

        final AtomicLong packetsProcessed = new AtomicLong();
        final AtomicLong packetsInCassandra = new AtomicLong();
        final AtomicLong packetsNotInCassandra = new AtomicLong();

        AisReader reader = AisReaders.createDirectoryReader(path, glob, recursive);
        if (tag != null) {
            reader.setSourceId(tag);
        }

        reader.registerPacketHandler(new Consumer<AisPacket>() {

            @Override
            public void accept(AisPacket p) {
                executorService.submit( () -> {
                    final long timestamp = p.getBestTimestamp();
                    final int timeBlock = timeBlock(table, Instant.ofEpochMilli(timestamp));
                    //final byte[] digest = digest(p);

                    Statement select = QueryBuilder.select()
                            .column(COLUMN_AISDATA.toString())
                            .from(table.toString())
                            .where(eq(COLUMN_TIMEBLOCK.toString(), timeBlock))
                            .and(eq(COLUMN_TIMESTAMP.toString(), timestamp))
                            .setConsistencyLevel(cl);
                    //.and(eq(COLUMN_AISDATA_DIGEST.toString(), ByteBuffer.wrap(digest)));

                    ResultSetFuture resultSetFuture = session.executeAsync(select);
                    ResultSet resultSet = resultSetFuture.getUninterruptibly();

                    if (resultSet.one() == null) {
                        packetsNotInCassandra.incrementAndGet();
                        System.out.println(p.getStringMessage());
                    } else {
                        packetsInCassandra.incrementAndGet();
                    }

                    long pProcessed = packetsProcessed.incrementAndGet();
                    if (pProcessed%10000L == 0) {
                        LOG.debug(formatNumberOfInflightQueries(session));
                        if (executorService instanceof ThreadPoolExecutor) {
                            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
                            LOG.debug("Query threads: " + threadPoolExecutor.getActiveCount() + " running; " + threadPoolExecutor.getCompletedTaskCount() + " completed; " + threadPoolExecutor.getQueue().size() + " queued.");
                        }
                    }
                    if (pProcessed%100000L == 0) {
                        LOG.debug(pProcessed + " packets processed.");
                    }
                });
            }
        });

        reader.start();
        reader.join();
        executorService.awaitTermination(365, TimeUnit.DAYS);

        LOG.info("Done. " + packetsInCassandra.longValue() + "/" + packetsNotInCassandra.longValue() + "/" + packetsProcessed.longValue());
    }

    private String formatNumberOfInflightQueries(Session session) {
        StringBuilder sb = new StringBuilder("Inflight queries: ");
        Collection<Host> connectedHosts = session.getState().getConnectedHosts();
        connectedHosts.forEach(host -> sb.append(host.getAddress().getHostAddress() + ": " + session.getState().getInFlightQueries(host) + " "));
        return sb.toString();
    }

    private static ConsistencyLevel toConsistencyLevel(String consistencyLevel) {
          switch (consistencyLevel.trim().toUpperCase()) {
              case "ANY": return ConsistencyLevel.ANY;
              case "ONE": return ConsistencyLevel.ONE;
              case "TWO": return ConsistencyLevel.TWO;
              case "THREE": return ConsistencyLevel.THREE;
              case "QUORUM": return ConsistencyLevel.QUORUM;
              case "LOCAL_QUORUM": return ConsistencyLevel.LOCAL_QUORUM;
              case "EACH_QUORUM": return ConsistencyLevel.EACH_QUORUM;
              case "SERIAL": return ConsistencyLevel.SERIAL;
              case "LOCAL_SERIAL": return ConsistencyLevel.LOCAL_SERIAL;
              case "LOCAL_ONE": return ConsistencyLevel.LOCAL_ONE;
              default: return null;
          }
    }

    public static void main(String[] args) throws Exception {
        // args = AisReaders.getDefaultSources();
        if (args.length == 0) {
            System.err.println("Must specify at least 1 source (sourceName=host:port,host:port sourceName=host:port)");
            System.exit(1);
        }
        new FileDiff().execute(args);
    }
}
