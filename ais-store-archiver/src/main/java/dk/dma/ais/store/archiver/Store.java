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
package dk.dma.ais.store.archiver;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPackets;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisTcpReader;
import dk.dma.ais.store.cassandra.CassandraAisStoreSchema;
import dk.dma.ais.store.cassandra.support.KeySpaceConnection;
import dk.dma.commons.app.AbstractDaemon;
import dk.dma.commons.management.ManagedAttribute;
import dk.dma.commons.management.ManagedResource;
import dk.dma.commons.service.AbstractBatchedStage;
import dk.dma.commons.service.io.FileWriterService;
import dk.dma.enav.util.function.Consumer;

/**
 * 
 * @author Kasper Nielsen
 */
@ManagedResource
public class Store extends AbstractDaemon {

    /** The file naming scheme for writing backup files. */
    static final String BACKUP_FORMAT = "'ais-store-failed' yyyy.MM.dd HH:mm'.txt.zip'";

    /** The number of packets we try to write at a time. */
    static final int BATCH_SIZE = 500;

    @Parameter(names = "-backup", description = "The backup directory")
    File backup = new File("aisbackup");

    @Parameter(names = "-database", description = "The cassandra database to write data to")
    String cassandraDatabase = "aisdata";

    @Parameter(names = "-hosts", description = "A list of cassandra hosts that can store the data")
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
        // setup an AisReader for each source
        Map<String, AisTcpReader> readers = AisTcpReader.parseSourceList(sources);

        // Starts the backup service that will write files to disk if disconnected
        final AbstractBatchedStage<AisPacket> fileWriter = start(FileWriterService.dateService(backup.toPath(),
                BACKUP_FORMAT, AisPackets.OUTPUT_TO_TEXT));

        // Setup keyspace for cassandra
        KeySpaceConnection con = start(KeySpaceConnection.connect(cassandraDatabase, cassandraSeeds));

        // Start a stage that will write each packet to cassandra
        final AbstractBatchedStage<AisPacket> cassandra = mainStage = start(con.createdBatchedStage(BATCH_SIZE,
                new CassandraAisStoreSchema()));

        // Start the thread that will read each file from the backup queue
        start(new FileImport(this));

        for (AisReader reader : readers.values()) {
            start(ArchiverUtil.wrapAisReader(reader, new Consumer<AisPacket>() {
                @Override
                public void accept(AisPacket aisPacket) {
                    // We use offer because we do not want to block receiving
                    if (!cassandra.getInputQueue().offer(aisPacket)) {
                        if (!fileWriter.getInputQueue().offer(aisPacket)) {
                            System.err.println("Could not persist packet");
                        }
                    }
                }
            }));
        }
    }

    public static void main(String[] args) throws Exception {
        // args = new String[] { "-source", "ais163.sealan.dk:65262", "-store", "localhost" };
        // args = new String[] { "src1=ais163.sealan.dk:65262,ais167.sealan.dk:65261",
        // "src2=iala63.sealan.dk:4712,iala68.sealan.dk:4712", "src3=10.10.5.144:65061" };
        new Store().execute(args);
    }
}
