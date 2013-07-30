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
package dk.dma.ais.store;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketInputStream;
import dk.dma.ais.store.write.DefaultAisStoreWriter;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.service.AbstractBatchedStage;

/**
 * 
 * @author Kasper Nielsen
 */
class FileImport extends AbstractCommandLineTool {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(FileImport.class);

    private static final int SIZE = 10 * Archiver.BATCH_SIZE;

    @Parameter(required = true, description = "files to import...")
    List<String> sources;

    /** Where files should be moved to after having been processed. */
    Path moveTo;

    @Parameter(names = "-databaseName", description = "The cassandra database to write data to")
    String cassandraDatabase = "aisdata";

    @Parameter(names = "-database", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        AisStoreConnection con = start(AisStoreConnection.create(cassandraDatabase, cassandraSeeds));

        final AbstractBatchedStage<AisPacket> cassandra = start(new DefaultAisStoreWriter(con, SIZE) {
            public void onFailure(List<AisPacket> messages, Exception cause) {
                LOG.error("Could not write batch to cassandra", cause);
                shutdown();
            }
        });

        start(cassandra);

        Set<Path> paths = new HashSet<>();
        for (String s : sources) {
            Path path = Paths.get(s);
            if (paths.add(path)) {
                int count = 0;
                LOG.info("Starting processing file " + path);
                try (AisPacketInputStream apis = AisPacketInputStream.createFromFile(path, true)) {
                    AisPacket p;
                    while ((p = apis.readPacket()) != null) {
                        count++;
                        cassandra.getInputQueue().put(p);
                    }
                }
                LOG.info("Finished processing file, " + count + " packets was imported from " + path);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new FileImport().execute(args);
    }
}
