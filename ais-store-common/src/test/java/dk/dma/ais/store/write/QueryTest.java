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
package dk.dma.ais.store.write;

import java.util.Date;

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

    public static void main(String[] args) {
        CassandraConnection con = CassandraConnection.create("aisdata", "10.3.240.204");
        //CassandraConnection con = CassandraConnection.create("aisdata", "192.168.56.101");

        con.start();
        try {
            int count = 0;

            // BoundingBox bb = BoundingBox.create(Position.create(-90, -180), Position.create(90, 180),
            // CoordinateSystem.CARTESIAN);

//            Iterable<AisPacket> iter = con.findForArea(bb, 0L, new Date().getTime());
//            Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder.forTime().setInterval(1375310431000L,
//                    15372286728091L));
            
            Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder.forTime().setInterval(new Date().getTime()-(1000*60*10),new Date().getTime()));
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
            con.stop();
        }
    }
}
