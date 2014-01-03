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

import java.nio.charset.StandardCharsets;
import java.util.Date;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.db.cassandra.CassandraConnection;

/**
 * @author jtj
 * 
 */
@SuppressWarnings("deprecation")
public class DumpTest {
    public static void main(String[] args) {
        // CassandraConnection con = CassandraConnection.create("aisdata",
        // "10.3.240.203");

        String keyspace = (args.length > 0) ? args[0] : "aisdata";
        String server = (args.length > 1) ? args[1] : "192.168.56.101";
        Integer daysBack = (args.length > 2) ? Integer.valueOf(args[2]) : 0;
        CassandraConnection con = CassandraConnection.create(keyspace, server);

        con.start();
        try {
            long count = 0;
            long totalSize = 0;
            long minSize = 999;
            long maxSize = 0;

            Long oneDay = (long) (1000 * 60 * 60 * 24);
            Long tf = new Date().getTime() - (oneDay * (daysBack + 1));
            Long now = new Date().getTime() - (oneDay * daysBack);

            Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder
                    .forTime().setInterval(tf, now));
            

            long start = System.currentTimeMillis();
            long ms;
            for (AisPacket p : iter) {
                long size = p.getStringMessage().getBytes(
                        StandardCharsets.US_ASCII).length;

                totalSize += size;
                minSize = Math.min(size, minSize);
                maxSize = Math.max(size, maxSize);

                count++;

                if (count % 100000 == 0) {
                    ms = System.currentTimeMillis() - start;
                    System.out.println(count + " packets,  " + count
                            / ((double) ms / 1000) + " packets/s");

                    System.out.println("AvgPackage Size: " + totalSize / count
                            + " bytes");
                    

                    System.out.println("Minimum Package Size: " + 
                           minSize + " bytes");
                    System.out.println("Maximum Package Size: " + 
                            maxSize + " bytes");

                }

            }

            ms = System.currentTimeMillis() - start;


            System.out.println();
            System.out.println("Result:");

            System.out.println("Average Packets per 1sec:\t\t\t" + count
                    / (24 * 60 * 60));
            System.out.println("Average Packets per 1min:\t\t\t" + count
                    / (24 * 60));
            System.out.println("Average Packets per 1hour:\t\t\t" + count
                    / 24);
            System.out.println("Total Packets   per 1day:\t\t\t" + count
                    + " packets");
            
            double sizePerDay = totalSize / 1024 / 1024;
            double sizePerS = sizePerDay/(24*60*60);
            double sizePerHour = totalSize / 1024 / 1024 / 24;

            System.out.println("With Average packet size in bytes:\t\t\t"
                    + totalSize / count);
            
            System.out.println("Estimated size (extracted) 1s:\t\t\t"
                    + sizePerS + " MiB");
            System.out.println("Estimated size (extracted) 1hour:\t\t\t"
                    + sizePerHour + " MiB");
            System.out.println("Estimated size (extracted) 1day:\t\t\t"
                    + sizePerDay + " MiB");
            System.out.println("Estimated size (extracted) 1month:\t\t\t"
                    + (sizePerDay * 31) / 1024 + " GiB");
            System.out.println("Estimated size (extracted) 1year:\t\t\t"
                    + (sizePerDay * 365) / 1024 + " GiB");            

            double packetsPerMS = count / (double) ms;
            double packetsPerS = count / ((double) ms / 1000);
            double packetsPerMin = count / ((double) ms / 1000 / 60);
            double packetsPerHour = count / ((double) ms / 1000 / 60 / 60);
            double packetsPerDay = count / ((double) ms / 1000 / 60 / 60 / 24);

            
            System.out.println("Read Speed:");
            System.out.println("Average Packets per 1sec:\t\t\t" + packetsPerS);
            System.out.println("Average Packets per 1min:\t\t\t"
                    + packetsPerMin);
            System.out.println("Average Packets per 1hour:\t\t\t"
                    + packetsPerHour);
            System.out.println("Total Packets   per 1day:\t\t\t"
                    + packetsPerDay + " packets");

            System.out.println("Read Speed Ratio:\t\t\t\t"
                    + (packetsPerDay / count));

            double timeToExtract24hInSec = (double) ms / 1000;
            double timeToExtract1Hour = timeToExtract24hInSec / 24;
            double timeToExtract1Min = timeToExtract1Hour / 60;

            System.out.println("Average Time To Extract 1year:\t\t\t"
                    + (timeToExtract24hInSec * 365) / 60 / 60 + " hours");
            System.out.println("Average Time To Extract 1month:\t\t\t"
                    + (timeToExtract24hInSec * 31) / 60 / 60 + " hours");
            System.out.println("Average Time To Extract 1day:\t\t\t"
                    + timeToExtract24hInSec / 60 + " minutes");
            System.out.println("Average Time To Extract 1hour:\t\t\t"
                    + timeToExtract1Hour + " sec");
            System.out.println("Average Time To Extract 1min:\t\t\t"
                    + timeToExtract1Min + " sec");

            double packageSize = totalSize/count;
            double bytesPerS = packetsPerS*packageSize;
            
            System.out.println("Bandwith:");
            System.out.println("Average Bandwith:\t\t\t"
                    + bytesPerS/1024/1024 + " MiB/s");
            System.out.println("Average Bandwith:\t\t\t"
                    + (bytesPerS*60)/1024/1024 + " MiB/hour");
                    

        } finally {
            con.stop();
        }
    }
}
