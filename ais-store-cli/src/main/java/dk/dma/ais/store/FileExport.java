/* Copyright (c) 2011 Danish Maritime Authority.
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
package dk.dma.ais.store;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.store.exporter.RawSSTableAccessor;
import dk.dma.commons.app.AbstractCommandLineTool;

/**
 * 
 * @author Kasper Nielsen
 */
public class FileExport extends AbstractCommandLineTool {

    @Parameter(names = "-database", description = "The cassandra database to write data to")
    String cassandraDatabase = "aisdata";

    @Parameter(names = "-sourceFilter", description = "The sourceFilter to apply")
    String sourceFilter;

    @Parameter(names = "-interval", description = "The ISO 8601 time interval for data to export")
    String interval;

    @Parameter(names = "cassandraConfig", description = "cassandra.yaml")
    String cassandraYaml = "/Applications/apache-cassandra-1.2.5/conf/cassandra.yaml";

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        RawSSTableAccessor a = new RawSSTableAccessor(cassandraYaml, cassandraDatabase);
        a.process(null, null);
        System.exit(0);// Cassandra internals are a mess, no support for shutting it down without system.exit
    }

    public static void main(String[] args) throws Exception {
        new FileExport().execute(args);
    }
}
