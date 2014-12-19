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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.reader.AisReader;
import dk.dma.ais.reader.AisReaders;
import dk.dma.ais.store.write.DefaultAisStoreWriter;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.service.AbstractBatchedStage;
import dk.dma.db.cassandra.CassandraConnection;

/**
 * 
 * @author Kasper Nielsen
 * @author Jens Tuxen
 */
public class FileImport extends AbstractCommandLineTool {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(FileImport.class);

    @Parameter(names = "-batchSize", description = "The number of messages to write to cassandra at a time")
    int batchSize = 3000;

    @Parameter(names = {"-import", "-input", "-i"}, description = "Path to directory with files to import", required = true)
    String path;
    
    @Parameter(names = "-glob", description = "pattern for files to read (default *)")
    String glob = "*";
    
    @Parameter(names = "-recursive", description = "recursive directory reader")
    boolean recursive = true;
        

    @Parameter(names = "-keyspace", description = "The cassandra database to write data to")
    String cassandraDatabase = "aisdata";

    @Parameter(names = "-seeds", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");
    
    @Parameter(names = "-tag", description = "Overwrite the tag")
    String tag;
    
    @Parameter(names = "-rate", description = "Set desired import rate in packets/second")
    Long rate = 0L;
    
    @Parameter(names = "-verbose", description = "verbose prints packets/second stats")
    boolean verbose;
    

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        CassandraConnection con = start(CassandraConnection.create(cassandraDatabase, cassandraSeeds));
        
        
        final AtomicInteger acceptedCount = new AtomicInteger();
        
        final long start = System.currentTimeMillis();

        final AbstractBatchedStage<AisPacket> cassandra = start(new DefaultAisStoreWriter(con, batchSize) {
            public void onFailure(List<AisPacket> messages, Throwable cause) {
                LOG.error("Could not write batch to cassandra", cause);
                shutdown();
            }
        });
      
        AisReader reader = AisReaders.createDirectoryReader(path, glob, recursive);
        
        if (tag != null) {
            reader.setSourceId(tag);
        }
        
        
        reader.registerPacketHandler(new Consumer<AisPacket>() {

            @Override
            public void accept(AisPacket p) {
                    try {
                        while(!cassandra.getInputQueue().offer(p)) {
                            Thread.sleep(2000);
                            LOG.debug("waiting for queue to open");
                        }
                        
                        acceptedCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        LOG.debug("failed to sleep (cassandra input queue was full and sleep was interrupted)");
                    }
                    
                }
            });
                    
        //print stats if verbose
        if (verbose) {
            final AtomicInteger verboseCounter = new AtomicInteger();
            reader.registerPacketHandler(new Consumer<AisPacket>() {
                
                
                @Override
                public void accept(AisPacket arg0) {
                    
                    long count = verboseCounter.incrementAndGet();
                    if (count % 10000 == 0) {
                        long end = System.currentTimeMillis();
                        LOG.info("Average Import rate "+(double)count/((double)(end-start)/1000.0) +" packets/s");
                    }
                    
                }
            });
        }
                    
        //Gate packet reading speed by blocking for 1 second every x packets
        if (rate > 0L) {
            reader.registerPacketHandler(new Consumer<AisPacket>() {

                @Override
                public void accept(AisPacket arg0) {
                    if (acceptedCount.get() % rate == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            LOG.debug("failed to block reader (sleep interrupted)");
                        }
                    }
                    
                }
                
            });
            
            
        }
        
        reader.start();
        reader.join();
        LOG.info("Finished processing directory, " + acceptedCount + " packets was imported from " + path);
        
        
    }

    public static void main(String[] args) throws Exception {
        new FileImport().execute(args);
    }
}
