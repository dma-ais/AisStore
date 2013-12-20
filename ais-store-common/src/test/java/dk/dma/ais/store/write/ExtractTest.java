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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import org.junit.Ignore;
import org.junit.Test;

import dk.dma.ais.filter.LocationFilter;
import dk.dma.ais.filter.MessageFilterBase;
import dk.dma.ais.filter.SanityFilter;
import dk.dma.ais.message.IPositionMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketFilters;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.commons.util.Iterables;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.model.geometry.BoundingBox;
import dk.dma.enav.model.geometry.CoordinateSystem;
import dk.dma.enav.model.geometry.Position;
import dk.dma.enav.util.function.Predicate;

/**
 * 
 * @author Kasper Nielsen
 */
@SuppressWarnings(value = { "deprecation" })
public class ExtractTest {

    @Test
    @Ignore
    public void ignore() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        //CassandraConnection con = CassandraConnection.create("aisdata", "10.3.240.203");
        CassandraConnection con = CassandraConnection.create("aisdata", "10.3.240.204");
        con.start();
        try {
            
            OutputStreamSink<AisPacket> sink = AisPacketOutputSinks.OUTPUT_PREFIXED_SENTENCES;
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File("aisTEST.txt")));
            
            
            
            int count = 0;
            //BoundingBox bb = BoundingBox.create(Position.create(48, -180), Position.create(90, 180), CoordinateSystem.CARTESIAN);
            
            Date sd = new Date(2013-1900,6,1);
            Date ed = new Date(2013-1900,8,1);
            
            Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder.forTime().setInterval(sd.getTime(), ed.getTime()));
            //Iterable<AisPacket> iterRegion = AisPacketFilters.filterOnMessagePositionWithin(bb);
            //Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder.forArea(bb).setInterval(sd.getTime(), ed.getTime()));
            //Thread.sleep(120000);
            //iter.iterator().hasNext();
            
            Iterable<AisPacket> filteredPosition = Iterables.filter(iter, AisPacketFilters.filterOnMessageType(IPositionMessage.class));
            Iterable<AisPacket> filteredAboveFortyEight = Iterables.filter(filteredPosition, AisPacketFilters.filterOnMessageType(IPositionMessage.class));
            
            //SanityFilter sanity = new SanityFilter();
            LocationFilter location = new LocationFilter();
            
            location.addFilterGeometry(new Predicate<Position>() {

                @Override
                public boolean test(Position arg0) {
                    return (arg0.getLatitude() >= 48.0F);
                }
                
            });

            long start = System.currentTimeMillis();
            for (AisPacket p : filteredAboveFortyEight) {
                if (p != null && p.getVdm() != null && p.getTimestamp() != null) {
                    if (!location.rejectedByFilter(p)){
                        sink.process(bos,p,count);
                        count++;
                    }
                    
                    
                    if (count % 10000 == 0) {
                        long ms = System.currentTimeMillis() - start;
                        System.out.println(count + " packets,  " + count / ((double) ms / 1000) + " packets/s");
                        System.out.println("Timestamp: "+p.getBestTimestamp());
                    }

                }
            }
            long ms = System.currentTimeMillis() - start;
            System.out.println("Total: " + count + " packets,  " + count / ((double) ms / 1000) + " packets/s");
        } finally {
            con.stop();
        }
    }
}
