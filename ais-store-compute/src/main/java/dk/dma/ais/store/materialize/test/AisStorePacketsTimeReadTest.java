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
package dk.dma.ais.store.materialize.test;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.ais.store.materialize.TimeScan;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.db.cassandra.CassandraConnection;

@SuppressWarnings("deprecation")
public class AisStorePacketsTimeReadTest extends TimeScan {
    @Parameter(names = "-csv", required = false, description = "absolute path to csv result")
    protected String csvString = "AisStorePacketsTimeReadTest.csv";

    protected AtomicInteger count = new AtomicInteger();
    BufferedOutputStream bos;
    PrintWriter csv;
    OutputStreamSink<AisPacket> sink;
    protected CassandraConnection con;

    @Override
    public void run(Injector arg0) throws Exception {
        con = CassandraConnection.create(keySpace, hosts);
        con.start();

        bos = new BufferedOutputStream(new NullOutputStream());
        csv = new PrintWriter(new BufferedOutputStream(new FileOutputStream(
                csvString)));

        sink = AisPacketOutputSinks.OUTPUT_TO_TEXT;

        try {

            setStartTime(System.currentTimeMillis());
            Iterable<AisPacket> iter = makeRequest();

            for (AisPacket p : iter) {
                this.accept(p);

                if (count.getAndIncrement() % batchSize == 0) {

                    long ms = System.currentTimeMillis() - startTime;
                    System.out
                            .println(count.get() + " packets,  " + count.get()
                                    / ((double) ms / 1000) + " packets/s");
                }

            }

            setEndTime(System.currentTimeMillis());
            long ms = System.currentTimeMillis() - startTime;
            long s = ms / 1000;

            System.out.println();
            System.out.println("Result:");

            System.out.println("Total Packets   per 1day:\t" + count.get()
                    + " packets");
            System.out.println("Average Packets per 1h:\t" + count.get() / 24);
            System.out.println("Average Packets per 1min:\t " + count.get()
                    / 24 / 60);
            System.out.println("Average Packets per 1sec:\t " + count.get()
                    / 24 / 60 / 60);

            System.out.println("Read Speed:");
            System.out.println("Average Packets per 1day:\t" + count.get() / s
                    * 60 * 24 + " packets/day");
            System.out.println("Average Packets per 1h:\t" + count.get() / s
                    * 60 * 60 + " packets/h");
            System.out.println("Average Packets per 1min:\t" + count.get() / s
                    * 60 + " packets/min");
            System.out.println("Average Packets per 1sec:\t" + count.get() / s
                    + " packets/s");

            System.out.println("Read/Write ratio:\t" + s * 60 * 24
                    / count.get() + "");
            System.out.println("Total Time To Extract 1day:\t" + s / 60
                    + " minutes");
            System.out.println("Total Time To Extract 1h:\t" + s * 60 * 24
                    / count.get() + "");

            csv.print(this.toCSV());
            

        } finally {
            con.stop();
        }
    }


    public static void main(String[] args) throws Exception {
        new AisStorePacketsTimeReadTest().execute(args);
    }

    @Override
    public void accept(AisPacket arg0) {
        try {
            this.process(bos, arg0, count.get());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void process(BufferedOutputStream bos, AisPacket p, long count)
            throws IOException {
        sink.process(bos, p, count);
    }

    @Override
    protected void buildView() {
        // TODO Auto-generated method stub

    }
    
    /** Writes to nowhere */
    class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            b++; //do something with the bytes just to be sure
        }
    }


}
