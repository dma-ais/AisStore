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
import dk.dma.ais.store.AisQueries;
import dk.dma.ais.store.AisQueryEngine;
import dk.dma.ais.store.cassandra.CassandraAisQueryEngine;
import dk.dma.commons.app.AbstractDaemon;
import dk.dma.db.cassandra.KeySpaceConnection;
import dk.dma.enav.model.geometry.BoundingBox;
import dk.dma.enav.model.geometry.CoordinateSystem;
import dk.dma.enav.model.geometry.Position;

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
        AisQueryEngine mqs = new CassandraAisQueryEngine(con);
        // Start a stage that will write each packet to cassandra

        long start = System.currentTimeMillis();
        // 219000368
        // mqs.findByMMSI("PT120H", 219000368).streamResults(System.out, AisPackets.OUTPUT_TO_HTML).get();

        BoundingBox bb = BoundingBox.create(Position.create(-40, 15), Position.create(12, 77),
                CoordinateSystem.CARTESIAN);

        // bb = BoundingBox.create(Position.create(54, 10), Position.create(56, 14), CoordinateSystem.CARTESIAN);

        mqs.findByArea(bb, AisQueries.toInterval(50, TimeUnit.DAYS))
                .streamResults(System.out, AisPackets.OUTPUT_TO_HTML).get();

        System.out.println(System.currentTimeMillis() - start);
        con.stop();
    }

    public static void main(String[] args) throws Exception {

        // System.out.println(Position.create(-40, 15));
        // System.out.println(bb);

        new AisQueryTester().execute(args);
        // System.out.println(Position.create(55.7200, 12.5700));

        // 55.7200° N, 12.5700° E
    }
}
