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
package dk.dma.ais.store.materialize.cli;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.Cluster;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.ais.reader.AisReaderGroup;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.store.Archiver;
import dk.dma.ais.store.FileImportService;
import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.write.MonitoredAisStoreWriter;
import dk.dma.commons.management.ManagedAttribute;
import dk.dma.commons.management.ManagedResource;
import dk.dma.commons.service.AbstractBatchedStage;
import dk.dma.commons.service.io.MessageToFileService;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.util.function.Consumer;

/**
 * Based on the Archiver from dk.dma.ais.store
 * 
 * @author Jens Tuxen
 */
@ManagedResource
public class MonitoredArchiver extends Archiver {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(MonitoredArchiver.class);

    /** The file naming scheme for writing backup files. */
    static final String BACKUP_FORMAT = "'ais-store-failed' yyyy.MM.dd HH:mm'.txt.zip'";

    @Parameter(names = "-viewHosts", required = false, description = "hosts in the format host:port,host:port")
    protected List<String> viewHosts;

    private Cluster viewCluster;

    @Parameter(names = "-viewKeyspace", required = false, description = "keyspace for the views")
    protected String viewKeySpace = AisMatSchema.VIEW_KEYSPACE;


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
        
        viewCluster = Cluster.builder().addContactPoints(viewHosts.toArray(new String[0])).build();

        // Starts the backup service that will write files to disk if disconnected
        final MessageToFileService<AisPacket> backupService = start(MessageToFileService.dateTimeService(
                backup.toPath(), BACKUP_FORMAT, AisPacketOutputSinks.OUTPUT_TO_TEXT));

        // setup an AisReader for each source
        AisReaderGroup g = AisReaders.createGroup("AisStoreArchiver", sources);

        // Start a stage that will write each packet to cassandra
        // this cassandra stage is monitored
        final AbstractBatchedStage<AisPacket> cassandra = mainStage = start(new MonitoredAisStoreWriter(con, batchSize, viewCluster, viewKeySpace) {
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
        new MonitoredArchiver().execute(args);
    }
}
