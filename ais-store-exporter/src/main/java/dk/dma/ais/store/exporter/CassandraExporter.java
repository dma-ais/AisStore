/*
 * Copyright (c) 2008 Kasper Nielsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.dma.ais.store.exporter;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import com.beust.jcommander.Parameter;

import dk.dma.app.management.ManagedAttribute;
import dk.dma.app.management.ManagedResource;

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
