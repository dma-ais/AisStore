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
import dk.dma.ais.reader.IAisPacketHandler;
import dk.dma.ais.store.cassandra.FullSchema;
import dk.dma.app.AbstractDaemon;
import dk.dma.app.cassandra.KeySpaceConnection;
import dk.dma.app.service.AbstractBatchedStage;

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
        start(AisTool.wrapAisReader(r, new IAisPacketHandler() {
            @Override
            public void receivePacket(AisPacket aisPacket) {
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
