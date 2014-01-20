package org.ais.store.compute;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.commons.util.io.OutputStreamSink;

public class AisStorePacketsTimeReadTest {
    public static void main(String[] args) {
        // CassandraConnection con = CassandraConnection.create("aisdata",
        // "10.3.240.203");
        Cluster cluster = Cluster.builder().addContactPoint("192.168.56.101")
                .build();

        Session session = cluster.connect("aisdata");
        
        
        /**Writes to nowhere*/
        class NullOutputStream extends OutputStream {
          @Override
          public void write(int b) throws IOException {
          }
        }
        
        try {
            long count = 0;

            // BoundingBox bb = BoundingBox.create(Position.create(-90, -180),
            // Position.create(90, 180),
            // CoordinateSystem.CARTESIAN);

            // Iterable<AisPacket> iter = con.findForArea(bb, 0L, new

            Long tf = new Date().getTime() - (1000 * 60 * 60 * 24);
            Long now = new Date().getTime();

            BufferedOutputStream bos;
            bos = new BufferedOutputStream(new NullOutputStream());

            OutputStreamSink<AisPacket> sink = AisPacketOutputSinks.OUTPUT_TO_TEXT;

            
            AisStoreQueryBuilder b = AisStoreQueryBuilder
                    .forTime().setInterval(tf, now); 
            
            
            
            Iterator<AisPacket> iter = b.execute(session).iterator();
            
            long start = System.currentTimeMillis();
            long ms;
            while (iter.hasNext()) {
                try {
                    AisPacket p = iter.next();
                    sink.process(bos, p , count);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                count++;

                if (count % 1000 == 0) {
                    ms = System.currentTimeMillis() - start;
                    System.out.println(count + " packets,  " + count
                            / ((double) ms / 1000) + " packets/s");
                }

            }
            
            ms = System.currentTimeMillis()-start;
            
            System.out.println();
            System.out.println("Result:");
            
            System.out.println("Total Packets   per 1day:\t" + count + " packets");
            System.out.println("Average Packets per 1h:\t"+count/24);
            System.out.println("Average Packets per 1min:\t "+count/24/60);
            System.out.println("Average Packets per 1sec:\t "+count/24/60/60);
            
            System.out.println("Read Speed:");
            System.out.println("Average Packets per 1day:\t"+ count / ((double) ms / 1000)*60*24 + " packets/day");
            System.out.println("Average Packets per 1h:\t"+  count / ((double) ms / 1000)*60*60 + " packets/h");
            System.out.println("Average Packets per 1min:\t"+  count / ((double) ms / 1000)*60 + " packets/min");
            System.out.println("Average Packets per 1sec:\t"+ count / ((double) ms / 1000) + " packets/s");
            
            System.out.println("Read/Write ratio:\t"+ (((double) ms / 1000)*60*24)/count + "");
            System.out.println("Total Time To Extract 1day:\t"+ (double) ms / 1000 / 60  + " minutes");
            System.out.println("Total Time To Extract 1h:\t"+ (((double) ms / 1000)*60*24)/count + "");
            
        } finally {
            session.shutdown();
        }
    }
}
