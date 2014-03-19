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

/**
 * @author Jens Tuxen
 */
package dk.dma.ais.store.materialize.raw.jobs;

import static dk.dma.ais.store.AisStoreSchema.TABLE_TIME;
import static dk.dma.ais.store.AisStoreSchema.TABLE_MMSI;

import java.io.EOFException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import javax.naming.ConfigurationException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.old.exporter.CassandraNodeTool;
import dk.dma.enav.util.function.EConsumer;
import dk.dma.enav.util.function.Function;

/**
 * @author jtj
 * 
 */
public class CountMMSIAisRaw extends SimpleCassandraColumnFamilyProcessor
        implements Runnable {
    long count;
    private final long start;
    int currentMMSI;

    /**
     * @param yamlUrl
     * @param keyspace
     */
    public CountMMSIAisRaw(String yamlUrl, String keyspace) {
        super(yamlUrl, keyspace);
        start = System.currentTimeMillis();
    }

    public Function<AisPacket, Integer> getApplyFunction() {
        return new MMSICount();

    }



    public synchronized void increment() {
        count++;
    }

    public synchronized long getCount() {
        return count;
    }

    protected void processDataFileLocations(String snapshotName,
            EConsumer<AisPacket> producer, String tableName) throws Exception {
        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            Path snapshots = Paths.get(s).resolve(keyspace).resolve(tableName)
                    .resolve("snapshots").resolve(snapshotName);
            // iterable through all data files (xxxx-Data)
            // if the dataformat changes hf needs to be upgraded to the current
            // versino
            // http://svn.apache.org/repos/asf/cassandra/trunk/src/java/org/apache/cassandra/io/sstable/Descriptor.java
            System.out.println("heyo");
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(snapshots,
                    keyspace + "-" + tableName + "-jb-*-Data.db")) {
                for (Path p : ds) { // for each data file
                    try {
                        processDataFile(p, producer, tableName);
                        if (bytesRead.get() > maxRead) {
                            System.out
                                    .println("Read maximum limit of bytes, [max = "
                                            + maxRead + ", actual ="
                                            + bytesRead.get() + "]");
                            return;
                        }
                    } catch (EOFException e) {
                        System.out.println("faield to process "+p);
                    }
                    
                }
            }
        }
    }

    //TODO: broken since dependency update
    /*
    public static void main(String[] args) throws Exception {
        final CountMMSIAisRaw counMmsi = new CountMMSIAisRaw("cassandra.yaml",
                "aisdata");

        // Create a new immutable snapshot that we will operate on. This will
        // also flush all data to disk
        String snapshotName = new Date().toString().replace(' ', '_')
                .replace(':', '_');

        CassandraNodeTool node = new CassandraNodeTool();
        node.takeSnapshot("aisdata", snapshotName, "packets_mmsi");

        Thread.sleep(2000);

        new Thread(counMmsi).start();
        
        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemKeyspaces().size() < 1) {
            throw new ConfigurationException("no non-system tables are defined");
        }

        counMmsi.processDataFileLocations(snapshotName,
                new EConsumer<AisPacket>() {

                    @Override
                    public void accept(AisPacket t) throws Exception {
                        counMmsi.increment();
                    }
                }, "packets_mmsi");

    }*/

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000);
                long ms = System.currentTimeMillis() - start;
                System.out
                        .println(this.getCount() + " packets,  "
                                + this.getCount() / ((double) ms / 1000)
                                + " packets/s");

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
    
    public class MMSICount extends Function<AisPacket, Integer> {
        /*
         * (non-Javadoc)
         * 
         * @see dk.dma.enav.util.function.Function#apply(java.lang.Object)
         */
        @Override
        public Integer apply(AisPacket t) {
            return 1;
        }
    }
}
