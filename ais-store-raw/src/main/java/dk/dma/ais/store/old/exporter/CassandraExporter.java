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
package dk.dma.ais.store.old.exporter;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import com.beust.jcommander.Parameter;

import dk.dma.commons.management.ManagedAttribute;
import dk.dma.commons.management.ManagedResource;

/**
 * 
 * @author Kasper Nielsen
 */
@ManagedResource
public class CassandraExporter /* extends AbstractFileExporter<AisPacket> */{

    @Parameter(names = "-cassandra", description = "The full path to cassandra.yaml", required = true)
    File cassandra;

    volatile CassandraColumnFamilyProcessor csp;

    @Parameter(names = "-keyspace", description = "The keyspace to use")
    String keyspace = "DMA";

    @Parameter(names = "-limit", description = "The maximum number of megabytes read (usefull for tests )")
    long maxRead = Long.MAX_VALUE;

    public CassandraExporter() {
        // exporter = null;// DefaultExportFunction.class.getCanonicalName();
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());
    }

    @ManagedAttribute
    public long getNumberOfBytesRead() {
        return csp == null ? 0 : csp.bytesRead.get();
    }

    @ManagedAttribute
    public long getNumberOfMegaBytesRead() {
        return getNumberOfBytesRead() / 1024 / 1024;
    }

    // @Override
    // protected void traverseSourceData(BatchProcessor<AisPacket> producer) throws Exception {
    // csp = new CassandraColumnFamilyProcessor(cassandra.toString(), keyspace, maxRead * 1024 * 1024);
    // csp.process(producer);
    // }
    public static void main(String[] args) throws Exception {
        args = new String[] { "-cassandra", "/Applications/apache-cassandra-1.1.6/conf/cassandra.yaml", "-noZip",
                "-start", "2012-11-12", "-limit", "10" };
        // new CassandraExporter().execute(args);
    }
}
