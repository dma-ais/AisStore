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
package dk.dma.ais.store.importer;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.IOException;

/**
 * Creates an AisStore Table/Schema writer, see AisStoreTableWriters for implementation.
 *
 * See also http://www.datastax.com/dev/blog/using-the-cassandra-bulk-loader-updated
 *
 * @param types note: need to be aware of super composite keys as partition key, for instance.
 * @author Jens Tuxen
 *
 */
public abstract class AisStoreSSTableWriter {

    protected CQLSSTableWriter writer;

    public AisStoreSSTableWriter(String outputDir, String schema, String insertStmt) {
        writer = CQLSSTableWriter.builder()
            .inDirectory(outputDir)
            .forTable(schema)
            .withBufferSizeInMB(256)
            .using(insertStmt)
            .withPartitioner(new Murmur3Partitioner()).build();
    }

    public void close() throws IOException {
        writer.close();
    }

}
