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
package dk.dma.ais.store;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.db.cassandra.CassandraConnection;
@SuppressWarnings("deprecation")
public class DriverFileExport extends AbstractCommandLineTool {

    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(DriverFileExport.class);

    
    @Parameter(names = "-start", description = "[Filter] Start date (inclusive), format == yyyy-MM-dd")
    protected volatile Date start = new Date(Calendar.getInstance().getTimeInMillis()-3600000);

    @Parameter(names = "-stop", description = "[Filter] Stop date (exclusive), format == yyyy-MM-dd")
    protected volatile Date stop = Calendar.getInstance().getTime();

    @Parameter(names = "-hosts", description = "hosts in the format host:port,host:port")
    protected String host;

    /** Where files should be moved to after having been processed. */
    @Parameter(names = "-path", required = true, description = "Path to extract to")
    String outPath;

    
    @Parameter(names = "-outputFormat", required = false, description = "Output formats: [OUTPUT_TO_TEXT, OUTPUT_PREFIXED_SENTENCES, OUTPUT_TO_HTML]")
    String outputSinkFormat = "OUTPUT_TO_TEXT";
    
    @Parameter(names = "-keySpace", description = "keyspace for the aisdata")
    protected String keySpace = "aisdata";
    
    final Integer batchSize = 100000;
    
    @Override
    protected void run(Injector arg0) throws Exception {
        CassandraConnection con;
        int count = 0;

        con = CassandraConnection.create(keySpace, host);
        con.start();

        final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outPath));
        @SuppressWarnings("unchecked")
        final OutputStreamSink<AisPacket> sink = (OutputStreamSink<AisPacket>) AisPacketOutputSinks.class.getField(outputSinkFormat).get(null);
        
        Iterable<AisPacket> iter = con.execute(AisStoreQueryBuilder
                .forTime().setInterval(start.getTime(),
                        stop.getTime()));            


        long start = System.currentTimeMillis();
        try {
            for (AisPacket p : iter) {
                if (p != null) {
                    count++;
                    sink.process(bos,p,count);
                }
    
                if (count % batchSize == 0) {
                    long ms = System.currentTimeMillis() - start;
                    System.out.println(count + " packets,  " + count
                            / ((double) ms / 1000) + " packets/s");
                }
            }

            long ms = System.currentTimeMillis() - start;
            System.out.println("Total: " + count + " packets,  " + count
                    / ((double) ms / 1000) + " packets/s");

        
        } finally {
            con.stop();
            bos.close();
        }
        
    }


    
    

    public static void main(String[] args) throws Exception {
        new DriverFileExport().execute(args);
    }    

}
