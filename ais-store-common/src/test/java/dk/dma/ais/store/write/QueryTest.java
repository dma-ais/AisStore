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
import dk.dma.ais.store.AisStoreConnection;
import dk.dma.enav.model.geometry.BoundingBox;
import dk.dma.enav.model.geometry.CoordinateSystem;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
public class QueryTest {

    @Test
    @Ignore
    public void ignore() {}

    public static void main(String[] args) {
        AisStoreConnection con = AisStoreConnection.create("aisdata", "127.0.0.1");

        con.start();
        try {
            int count = 0;

            BoundingBox bb = BoundingBox.create(Position.create(-90, -180), Position.create(90, 180),
                    CoordinateSystem.CARTESIAN);

            Iterable<AisPacket> iter = con.findForArea(bb, 0L, new Date().getTime());
            long start = System.currentTimeMillis();
            for (AisPacket p : iter) {
                if (p != null) {
                    count++;
                }
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
