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

package org.ais.store.compute;

import java.util.Date;

import javax.naming.ConfigurationException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.compute.raw.jobs.SimpleCassandraColumnFamilyProcessor;
import dk.dma.ais.store.old.exporter.CassandraNodeTool;
import dk.dma.enav.util.function.EConsumer;

/**
 * @author jtj
 * 
 */
public final class FlatFileTest extends SimpleCassandraColumnFamilyProcessor
        implements Runnable {

    long count = 0;
    private final long start;

    /**
     * @param yamlUrl
     * @param keyspace
     */
    public FlatFileTest(String yamlUrl, String keyspace) {
        super(yamlUrl, keyspace);
        start = System.currentTimeMillis();
    }

    public final synchronized void increment() {
        count++;
    }

    public final synchronized long getCount() {
        return count;
    }

    public static void main(String[] args) throws Exception {
        final String keyspaceAisdata = "aisdata";
        String tableName = "packets_time";
        // Create a new immutable snapshot that we will operate on. This will
        // also flush all data to disk
        String snapshotName = new Date().toString().replace(' ', '_')
                .replace(':', '_');

        final FlatFileTest flatFileTest = new FlatFileTest("cassandra.yaml",
                keyspaceAisdata);

        CassandraNodeTool node = new CassandraNodeTool();

        node.takeSnapshot(keyspaceAisdata, snapshotName, tableName);

        new Thread(flatFileTest).start();

        try {
            DatabaseDescriptor.loadSchemas();
            if (Schema.instance.getNonSystemKeyspaces().size() < 1) {
                throw new ConfigurationException(
                        "no non-system tables are defined");
            }

            flatFileTest.processDataFileLocations(snapshotName,
                    new EConsumer<AisPacket>() {

                        @Override
                        public void accept(AisPacket t) throws Exception {
                            flatFileTest.increment();
                            // t.getAisMessage();
                        }
                    }, tableName);

        } finally {
            node.deleteSnapshot(keyspaceAisdata, snapshotName);
        }

    }

    /**
     * Handles printing of stats in separate thread
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

}
