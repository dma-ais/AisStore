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

import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.store.importer.AisStoreSSTableGenerator;
import dk.dma.ais.store.importer.ImportConfigGenerator;
import dk.dma.commons.app.AbstractCommandLineTool;

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
    

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        
        //bootstrap a valid cassandra.yaml config file into the inDirectory
        ImportConfigGenerator.generate(inDirectory);
        Properties props = System.getProperties();
        props.setProperty("cassandra.config", Paths.get("file://",inDirectory, "cassandra.yaml").toString());
        
        AisStoreSSTableGenerator gen = AisStoreSSTableGenerator
                .createAisStoreSSTableGenerator(inDirectory,keyspace,compressor, bufferSize);
        
        
        final AtomicInteger acceptedCount = new AtomicInteger();
        final long start = System.currentTimeMillis();
        AisReader reader = AisReaders.createDirectoryReader(path, glob,
                recursive);

        if (tag != null) {
            reader.setSourceId(tag);
        }

        // print stats if verbose
        if (verbose) {
            final AtomicInteger verboseCounter = new AtomicInteger();
            reader.registerPacketHandler(packet -> {

                long count = verboseCounter.incrementAndGet();
                if (count % 10000 == 0) {
                    long end = System.currentTimeMillis();
                    LOG.info("Average Conversion rate " + (double) count
                            / ((double) (end - start) / 1000.0) + " packets/s");
                }
            });
        }
        
        //add "accepted" counter
        reader.registerPacketHandler(p -> acceptedCount.incrementAndGet());
        
        reader.registerPacketHandler(gen);

        reader.start();
        reader.join();
        gen.close();
        LOG.info("Finished processing directory, " + acceptedCount
                + " packets was converted from " + path);
        
        shutdown();
        System.exit(0);
        
    }

    public static void main(String[] args) throws Exception {
        new FileSSTableConverter().execute(args);
    }
}
