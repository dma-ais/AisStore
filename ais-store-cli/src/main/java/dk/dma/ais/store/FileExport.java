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
