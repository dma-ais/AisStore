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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.ais.reader.AisReaderGroup;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.store.write.DefaultAisStoreWriter;
import dk.dma.commons.app.AbstractDaemon;
import dk.dma.commons.management.ManagedAttribute;
import dk.dma.commons.management.ManagedResource;
import dk.dma.commons.service.AbstractBatchedStage;
import dk.dma.commons.service.io.MessageToFileService;
import dk.dma.db.cassandra.CassandraConnection;

/**
 * 
 * @author Kasper Nielsen
 */
@ManagedResource
public class Archiver extends AbstractDaemon {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(Archiver.class);

    /** The file naming scheme for writing backup files. */
    static final String BACKUP_FORMAT = "'ais-store-failed' yyyy.MM.dd HH:mm'.txt.zip'";

    @Parameter(names = "-backup", description = "The backup directory")
    File backup = new File("aisbackup");

    @Parameter(names = "-databaseName", description = "The cassandra database to write data to")
    String databaseName = "aisdata";

    @Parameter(names = "-batchSize", description = "The number of messages to write to cassandra at a time")
    int batchSize = 1000;

    @Parameter(names = "-database", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    /** The stage that is responsible for writing the package */
    volatile AbstractBatchedStage<AisPacket> mainStage;

    @Parameter(description = "A list of AIS sources (sourceName=host:port,host:port sourceName=host:port ...")
    List<String> sources;

    @ManagedAttribute
    public long getNumberOfProcessedPackages() {
        AbstractBatchedStage<AisPacket> mainStage = this.mainStage;
        return mainStage == null ? 0 : mainStage.getNumberOfMessagesProcessed();
    }

    @ManagedAttribute
    public int getNumberOfOutstandingPackets() {
        AbstractBatchedStage<AisPacket> mainStage = this.mainStage;
        return mainStage == null ? 0 : mainStage.getSize();
    }

    /** {@inheritDoc} */
    @Override
    protected void runDaemon(Injector injector) throws Exception {
        // Setup keyspace for cassandra
        CassandraConnection con = start(CassandraConnection.create(databaseName, cassandraSeeds));

        // Starts the backup service that will write files to disk if disconnected
        final MessageToFileService<AisPacket> backupService = start(MessageToFileService.dateTimeService(
                backup.toPath(), BACKUP_FORMAT, AisPacketOutputSinks.OUTPUT_TO_TEXT));

        // setup an AisReader for each source
        AisReaderGroup g = AisReaders.createGroup("AisStoreArchiver", sources);

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
        });

        // Start the thread that will read each file from the backup queue
        start(new FileImportService(this));
        start(backupService.startFlushThread()); // we want to occasional flush and close dormant files

        g.stream().subscribe(new Consumer<AisPacket>() {
            public void accept(AisPacket aisPacket) {
                // We use offer because we do not want to block receiving
                if (!cassandra.getInputQueue().offer(aisPacket)) {
                    if (!backupService.getInputQueue().offer(aisPacket)) {
                        System.err.println("Could not persist packet");
                    }
                }
            }
        });
        start(g.asService());
    }

    public static void main(String[] args) throws Exception {
        // args = AisReaders.getDefaultSources();
        if (args.length == 0) {
            System.err.println("Must specify at least 1 source (sourceName=host:port,host:port sourceName=host:port)");
            System.exit(1);
        }
        new Archiver().execute(args);
    }
}
