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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.reader.AisStreamReader;
import dk.dma.ais.store.cassandra.FullSchema;
import dk.dma.commons.app.AbstractDaemon;
import dk.dma.commons.service.AbstractBatchedStage;
import dk.dma.db.cassandra.KeySpaceConnection;
import dk.dma.enav.util.function.Consumer;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisArchiverFromFile extends AbstractDaemon {

    @Parameter(names = "-backup", description = "The backup directory")
    File backup = new File("./aisbackup");

    @Parameter(names = "-backupformat", description = "The backup Format")
    String backupFormat = "yyyy/MM-dd/'ais-store-failed' yyyy.MM.dd HH:mm'.txt.zip'";

    @Parameter(names = "-store", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("10.10.5.201");

    @Parameter(names = "-source", description = "A list of AIS sources", required = true)
    List<String> sources;

    /** {@inheritDoc} */
    @Override
    protected void externalShutdown() {}

    /** {@inheritDoc} */
    @Override
    protected void runDaemon(Injector injector) throws Exception {

        // Setup keyspace for cassandra
        KeySpaceConnection con = start(KeySpaceConnection.connect("aisdata1", cassandraSeeds));

        // Start a stage that will write each packet to cassandra
        final AbstractBatchedStage<AisPacket> cassandra = start(con.createdBatchedStage(1000, FullSchema.INSTANCE));

        // setup AisReaders
        CountDownLatch cdl = new CountDownLatch(1);
        AisStreamReader r = new AisStreamReader(new BufferedInputStream(new FileInputStream(
                "/Users/kasperni/Downloads/dump.txt"), 100000));
        start(AisTool.wrapAisReader(r, new Consumer<AisPacket>() {
            @Override
            public void accept(AisPacket aisPacket) {
                // We use offer because we do not want to block receiving
                try {
                    cassandra.getInputQueue().put(aisPacket);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, cdl));
        cdl.await();
        cassandra.stopAndWait();
        shutdown();
    }

    public static void main(String[] args) throws Exception {
        args = new String[] { "-source", "ais163.sealan.dk:65262", "-store", "localhost" };
        new AisArchiverFromFile().execute(args);
    }
}
