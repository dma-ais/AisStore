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
import dk.dma.enav.model.geometry.Position;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

import static dk.dma.ais.store.AisStoreSchema.Table.TABLE_PACKETS_AREA_CELL1;
import static dk.dma.ais.store.AisStoreSchema.getDigest;
import static dk.dma.ais.store.AisStoreSchema.getTimeBlock;

/**
 * Creates an AisStore Table/Schema writer, see AisStoreTableWriters for implementation.
 *
 * See also
 *   - http://www.datastax.com/dev/blog/bulk-loading
 *   - https://github.com/yukim/cassandra-bulkload-example/blob/master/src/main/java/bulkload/BulkLoad.java
 *
 * @param types note: need to be aware of super composite keys as partition key, for instance.
 * @author Jens Tuxen
 *
 */
public class PacketsAreaCell1SSTableWriter extends AisStoreSSTableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PacketsAreaCell1SSTableWriter.class);

    public PacketsAreaCell1SSTableWriter(String outputDir, String keyspace) {
        super(
            outputDir,
            keyspace,
            String.format(
                "CREATE TABLE %s.%s (" +
                    "cellid int," +
                    "timeblock int," +
                    "time timestamp," +
                    "digest blob," +
                    "aisdata ascii," +
                    "PRIMARY KEY ((cellid, timeblock), time, digest)" +
                ") WITH CLUSTERING ORDER BY (time ASC, digest ASC)"
                , keyspace, TABLE_PACKETS_AREA_CELL1.toString()
            ),
            String.format(
                "INSERT INTO %s.%s (cellid, timeblock, time, digest, aisdata) VALUES (?, ?, ?, ?, ?)", keyspace, TABLE_PACKETS_AREA_CELL1.toString()
            )
        );

        // http://stackoverflow.com/questions/26137083/cassandra-does-cqlsstablewriter-support-writing-to-multiple-column-families-co
        KSMetaData ksm = Schema.instance.getKSMetaData(keyspace);
        Schema.instance.clearKeyspaceDefinition(ksm);
    }

    public void addPacket(AisPacket packet, Position p) {
        Objects.requireNonNull(packet);
        Objects.requireNonNull(p);

        final long ts = packet.getBestTimestamp();
        if (ts > 0) {
            final int cellid = p.getCellInt(1.0);
            try {
                writer.addRow(cellid, getTimeBlock(table(), Instant.ofEpochMilli(ts)), new Date(ts), ByteBuffer.wrap(getDigest(packet)), packet.getStringMessage());
            } catch (InvalidRequestException e) {
                LOG.error("Failed to store message in " + table().toString() + " due to " + e.getClass().getSimpleName() + ": " + e.getMessage());
            } catch (IOException e) {
                LOG.error("Failed to store message in " + table().toString() + " due to " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        } else {
            LOG.error("Cannot get timestamp from: " + packet.getStringMessage());
        }
    }

    @Override
    public Table table() {
        return TABLE_PACKETS_AREA_CELL1;
    }
}