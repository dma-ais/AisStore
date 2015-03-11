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

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.AisStoreSchema.Table;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

/**
 * Creates an AisStore Table/Schema writer, see AisStoreTableWriters for implementation.
 *
 * See also http://www.datastax.com/dev/blog/using-the-cassandra-bulk-loader-updated
 *
 * @param types note: need to be aware of super composite keys as partition key, for instance.
 * @author Jens Tuxen
 * @author Thomas Borg Salling
 */
@NotThreadSafe
public abstract class SSTableWriter implements Consumer<AisPacket> {

    private static final Logger LOG = LoggerFactory.getLogger(SSTableWriter.class);

    private CQLSSTableWriter writer;

    private long numberOfPacketsProcessed = 0L;

    private final Path writePath;
    private final String schemaDefinition;
    private final String insertStatement;

    public SSTableWriter(String outputDir, String keyspace, String schemaDefinition, String insertStatement) {
        this.schemaDefinition = schemaDefinition;
        this.insertStatement = insertStatement;
        this.writePath = writePath(outputDir, keyspace);
    }

    public void close() throws IOException {
        if (writer != null)
            writer.close();
    }

    public final long numberOfPacketsProcessed() {
        return numberOfPacketsProcessed;
    }

    protected final void incNumberOfPacketsProcessed() {
        numberOfPacketsProcessed++;
    }

    public abstract Table table();

    protected final CQLSSTableWriter writer() {
        if (writer == null) {
            createDirectories(writePath);
            LOG.info("Writing output to: " + writePath);

            writer =
                CQLSSTableWriter.builder()
                    .inDirectory(writePath.toString())
                    .forTable(schemaDefinition)
                    .withBufferSizeInMB(256)
                    .using(insertStatement)
                    .withPartitioner(new Murmur3Partitioner())
                    .build();
        }
        return writer;
    }

    private Path writePath(String directory, String keyspace) {
        return Paths.get(directory, keyspace, table().toString());
    }

    private static void createDirectories(Path path) {
        try {
            Files.createDirectories(path);
        } catch (FileAlreadyExistsException e) {
            LOG.warn(e.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

}
