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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


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
import dk.dma.enav.util.function.Consumer;

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

    @Parameter(required = true, description = "files to import...")
    List<String> sources;

    /** Where files should be moved to after having been processed. */
    Path moveTo;

    @Parameter(names = "-databaseName", description = "The cassandra database to write data to")
    String cassandraDatabase = "aisdata";

    @Parameter(names = "-database", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("localhost");
    
    @Parameter(names = "-tag", description = "Overwrite the tag")
    String tag;
    
    @Parameter(names = "-rate", description = "Set desired import rate in packets/second")
    Long rate = 0L;

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        CassandraConnection con = start(CassandraConnection.create(cassandraDatabase, cassandraSeeds));

        final AbstractBatchedStage<AisPacket> cassandra = start(new DefaultAisStoreWriter(con, batchSize) {
            public void onFailure(List<AisPacket> messages, Throwable cause) {
                LOG.error("Could not write batch to cassandra", cause);
                shutdown();
            }
        });


        Set<Path> paths = new HashSet<>();
        ArrayList<AisReader> readers = new ArrayList<>();
        for (String s : sources) {
            Path path = Paths.get(s);
            if (paths.add(path)) {
                final AtomicInteger count = new AtomicInteger();
                LOG.info("Starting processing file " + path);
                try {
                    AisReader apis = AisReaders.createReaderFromFile(path.toAbsolutePath().toString()); 
                    if (tag != null) {
                        apis.setSourceId(tag);
                    }
                    
                    apis.registerPacketHandler(new Consumer<AisPacket>() {

                        @Override
                        public void accept(AisPacket p) {
                            try {
                                while(!cassandra.getInputQueue().offer(p)) {
                                    Thread.sleep(2000);
                                }
                                
                                count.incrementAndGet();
                            } catch (InterruptedException e) {
                                LOG.debug("failed to sleep (cassandra input queue was full and sleep was interrupted)");
                            }
                            
                        }
                    });
                    
                    //Gate packet reading speed by blocking for 1 second every x packets
                    if (rate > 0L) {
                        apis.registerPacketHandler(new Consumer<AisPacket>() {

                            @Override
                            public void accept(AisPacket arg0) {
                                if (apis.getNumberOfLinesRead() % rate == 0) {
                                    try {
                                        Thread.sleep(1000);
                                    } catch (InterruptedException e) {
                                        LOG.debug("failed to block reader (sleep interrupted)");
                                    }
                                }
                                
                            }
                            
                        });
                        
                    }
                                        
                    readers.add(apis);
                    apis.start();
                    apis.join();
                    
                    
                } finally {
                    
                }
                
                LOG.info("Finished processing file, " + count + " packets was imported from " + path);
                
                
            }
        }
        
        //one at a time ladies.
        /*
        for (AisReader r: readers) {
            r.start();
            r.join();
        }
        */
        
    }

    public static void main(String[] args) throws Exception {
        new FileImport().execute(args);
    }
}
