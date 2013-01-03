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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPackets;
import dk.dma.ais.store.cassandra.CassandraMessageQueryService;
import dk.dma.ais.store.query.MessageQueryService;
import dk.dma.app.AbstractDaemon;
import dk.dma.app.cassandra.KeySpaceConnection;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisQueryTester extends AbstractDaemon {

    @Parameter(names = "-store", description = "A list of cassandra hosts that can store the data")
    List<String> cassandraSeeds = Arrays.asList("10.10.5.202");

    /** {@inheritDoc} */
    @Override
    protected void externalShutdown() {}

    /** {@inheritDoc} */
    @Override
    protected void runDaemon(Injector injector) throws Exception {
        // Setup keyspace for cassandra
        KeySpaceConnection con = start(KeySpaceConnection.connect("aisdata", cassandraSeeds));
        MessageQueryService mqs = new CassandraMessageQueryService(con);
        // Start a stage that will write each packet to cassandra

        long start = System.currentTimeMillis();
        int count = 0;
        // 219000368
        // mqs.findByMMSI("PT120H", 219000368).streamResults(System.out, AisPackets.OUTPUT_TO_HTML).get();

        mqs.findByShape(null, 100, TimeUnit.SECONDS).streamResults(System.out, AisPackets.OUTPUT_TO_HTML).get();

        System.out.println(System.currentTimeMillis() - start);
        con.stop();
    }

    public static void main(String[] args) throws Exception {
        new AisQueryTester().execute(args);
    }
}
