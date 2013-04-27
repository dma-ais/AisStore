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
