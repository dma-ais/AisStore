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
import com.google.inject.Injector;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.store.importer.ImportConfigGenerator;
import dk.dma.ais.store.importer.PacketsAreaCell10SSTableWriter;
import dk.dma.ais.store.importer.PacketsAreaCell1SSTableWriter;
import dk.dma.ais.store.importer.PacketsAreaUnknownSSTableWriter;
import dk.dma.ais.store.importer.PacketsMmsiSSTableWriter;
import dk.dma.ais.store.importer.PacketsTimeSSTableWriter;
import dk.dma.commons.app.AbstractCommandLineTool;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author Jens Tuxen
 */
public class FileSSTableConverter extends AbstractCommandLineTool {

    /** The logger. */
    static final Logger LOG = LoggerFactory
            .getLogger(FileSSTableConverter.class);

    @Parameter(names = "-keyspace", description = "The keyspace in cassandra")
    String keyspace = "aisdata";

    /**
     * Naming scheme "in directory" comes from Cassandra
     */
    @Parameter(names = { "-path", "-output", "-o" }, description = "path to extract to")
    String inDirectory;

    @Parameter(names = { "-import", "-input", "-i" }, description = "Path to directory with files to import", required = true)
    String path;

    @Parameter(names = "-glob", description = "pattern for files to read (default *)")
    String glob = "*";

    @Parameter(names = "-recursive", description = "recursive directory reader")
    boolean recursive = true;

    @Parameter(names = "-verbose", description = "verbose prints packets/second stats")
    boolean verbose;
    
    @Parameter(names = "-tag", description = "Overwrite the tag")
    String tag;
    
    @Parameter(names = "-compressor", description = "LZ4Compressor, DeflateCompressor")
    String compressor = "LZ4Compressor";
    
    @Parameter(names = "-bufferSize", description = "buffer size in mb (roughly the size of each flush to sstable, beware of heap usage, 128m ~ 1g heap")
    int bufferSize = 128;

    static {
        org.apache.cassandra.config.Config.setClientMode(true);
    }

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        
        //bootstrap a valid cassandra.yaml config file into the inDirectory
        ImportConfigGenerator.generate(inDirectory);
        Properties props = System.getProperties();
        props.setProperty("cassandra.config", Paths.get("file://", inDirectory, "cassandra.yaml").toString());

        Arrays.asList(
            new PacketsTimeSSTableWriter(inDirectory, keyspace),
            new PacketsMmsiSSTableWriter(inDirectory, keyspace),
            new PacketsAreaCell1SSTableWriter(inDirectory, keyspace),
            new PacketsAreaCell10SSTableWriter(inDirectory, keyspace),
            new PacketsAreaUnknownSSTableWriter(inDirectory, keyspace)
        ).forEach(sstableWriter -> {
            try {
                LOG.info("Streaming AIS packets to " + sstableWriter.table());
                streamAllAisPacketsTo(sstableWriter);
                sstableWriter.close();
                clearKeyspaceDefinition();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        });

        shutdown();
        System.exit(0);
    }


    private void streamAllAisPacketsTo(Consumer<AisPacket> consumer) throws IOException, InterruptedException {
        final AtomicLong acceptedCount = new AtomicLong();
        final AtomicLong[] numberOfPacketsProcessedSinceLastOutput = {new AtomicLong()};
        final Instant[] timeOfLastOutput = {Instant.now()};

        AisReader reader = AisReaders.createDirectoryReader(path, glob, recursive);

        if (tag != null) {
            reader.setSourceId(tag);
        }

        // print stats if verbose
        if (verbose) {
            reader.registerPacketHandler(packet -> {
                if (numberOfPacketsProcessedSinceLastOutput[0].incrementAndGet() % 1000000 == 0) {
                    Instant now = Instant.now();
                    Duration timeSinceLastOutput = Duration.between(timeOfLastOutput[0], now);

                    LOG.info("Conversion rate " + ((int) (numberOfPacketsProcessedSinceLastOutput[0].floatValue() / ((float) timeSinceLastOutput.toMillis())*1e3) + " packets/s, " + (acceptedCount.longValue()+1L) + " total packets processed."));

                    numberOfPacketsProcessedSinceLastOutput[0] = new AtomicLong();
                    timeOfLastOutput[0] = now;
                }
            });
        }

        //add "accepted" counter
        reader.registerPacketHandler(p -> acceptedCount.incrementAndGet());
        reader.registerPacketHandler(consumer);

        reader.start();
        reader.join();
        LOG.info("Finished processing directory, " + acceptedCount + " packets was converted from " + path);
    }

    private void clearKeyspaceDefinition() {
        // http://stackoverflow.com/questions/26137083/cassandra-does-cqlsstablewriter-support-writing-to-multiple-column-families-co
        KSMetaData ksm = Schema.instance.getKSMetaData(keyspace);
        Schema.instance.clearKeyspaceDefinition(ksm);
    }

    public static void main(String[] args) throws Exception {
        new FileSSTableConverter().execute(args);
    }
}
