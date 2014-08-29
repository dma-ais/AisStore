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
package dk.dma.ais.store.write;

import org.junit.Ignore;
import org.junit.Test;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.db.cassandra.CassandraConnection;

/**
 * 
 * @author Kasper Nielsen
 */
public class QueryTest {

    @Test
    @Ignore
    public void ignore() {}

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        CassandraConnection con = CassandraConnection.create("aisdata", "10.3.240.203");

        con.startAsync();
        try {
            int count = 0;

            // BoundingBox bb = BoundingBox.create(Position.create(-90, -180), Position.create(90, 180),
            // CoordinateSystem.CARTESIAN);

            // Iterable<AisPacket> iter = con.findForArea(bb, 0L, new Date().getTime());
            Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder.forTime().setInterval(1375310431000L,
                    15372286728091L));
            // iter.iterator().hasNext();

            long start = System.currentTimeMillis();
            for (AisPacket p : iter) {
                if (p != null) {
                    // System.out.println(p.getBestTimestamp());
                    // if (count == 0 || count == 188312) {
                    // System.out.println(String.format("count= %d, timestamp= %d", count, p.getBestTimestamp()));
                    // }
                    count++;

                }
                // System.out.println(p.getBestTimestamp());
                // System.out.println(p.getBestTimestamp() + " " + p.tryGetAisMessage().getValidPosition());
                if (count % 1000 == 0) {
                    long ms = System.currentTimeMillis() - start;
                    System.out.println(count + " packets,  " + count / ((double) ms / 1000) + " packets/s");
                }
            }
            long ms = System.currentTimeMillis() - start;
            System.out.println("Total: " + count + " packets,  " + count / ((double) ms / 1000) + " packets/s");
        } finally {
            con.stopAsync();
        }
    }
}
