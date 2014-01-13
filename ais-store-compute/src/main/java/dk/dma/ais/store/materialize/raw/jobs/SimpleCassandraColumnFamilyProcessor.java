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
package dk.dma.ais.store.compute.raw.jobs;

import static dk.dma.ais.store.AisStoreSchema.TABLE_TIME;
import static java.util.Objects.requireNonNull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

import com.google.common.util.concurrent.RateLimiter;

import dk.dma.ais.packet.AisPacket;
import dk.dma.enav.util.function.EConsumer;

/**
 * Processes Cassandras SSTables. Based on CassandraColumnFamilyProcessor,
 * forked because of evil plans. Modified to support table version jb and any
 * keyspace/columnfamily
 * 
 * @author jtj
 * 
 */
public class SimpleCassandraColumnFamilyProcessor {

    /** The key of the message column. */
    private static final byte[] MESSAGE_COLUMN = TABLE_TIME.getBytes();

    /** The number of bytes that have been read from disk. */
    protected final AtomicLong bytesRead = new AtomicLong();

    /** The name of the keyspace to use. */
    protected final String keyspace;

    /** The maximum number of bytes we want to read, useful for tests. */
    protected final long maxRead;

    /** If this processor has been consumed. */
    private final AtomicBoolean used = new AtomicBoolean();

    /** The URL to cassandra.yaml */
    private final String yamlUrl;

    public SimpleCassandraColumnFamilyProcessor(String yamlUrl, String keyspace) {
        this(yamlUrl, keyspace, Long.MAX_VALUE);
    }

    public SimpleCassandraColumnFamilyProcessor(String yamlUrl,
            String keyspace, long maxRead) {
        this.yamlUrl = requireNonNull(yamlUrl);
        this.keyspace = requireNonNull(keyspace);
        this.maxRead = maxRead;
    }

    protected void processDataFileLocations(String snapshotName,
            EConsumer<AisPacket> producer, String tableName) throws Exception {
        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            Path snapshots = Paths.get(s).resolve(keyspace).resolve(tableName)
                    .resolve("snapshots").resolve(snapshotName);

            try (DirectoryStream<Path> ds = Files.newDirectoryStream(snapshots,
                    keyspace + "-" + tableName + "-jb-*-Data.db")) {
                for (Path p : ds) { // for each data file
                    try {
                        processDataFile(p, producer, tableName);
                    } catch (EOFException e) {
                        System.out.println("failed to process " + p);
                    }

                }
            }
        }
    }

    protected Collection<SSTableReader> getReaders(String snapshotName,
            String tableName) throws IOException {
        ArrayList<SSTableReader> readers = new ArrayList<>();

        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            Path snapshots = Paths.get(s).resolve(keyspace).resolve(tableName)
                    .resolve("snapshots").resolve(snapshotName);

            try (DirectoryStream<Path> ds = Files.newDirectoryStream(snapshots,
                    keyspace + "-" + tableName + "-hf-*-Data.db")) {
                for (Path p : ds) { // for each data file
                    readers.add(SSTableReader.open(Descriptor.fromFilename(p
                            .toString())));
                }
            }
        }
        return readers;
    }

    protected void processDataFile(Path p, EConsumer<AisPacket> producer,
            String tableName) throws Exception {
        System.out.println(p.toString());
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(p
                .toString()));
        SSTableScanner scanner = reader.getScanner(RateLimiter
                .create(Double.MAX_VALUE));
        long sizeOnDisk = scanner.getLengthInBytes();
        System.out.println("Processing " + p + " [size = " + sizeOnDisk
                + ", uncompressed = " + reader.uncompressedLength() + "]");
        long start = System.nanoTime();
        try {
            long previousPosition = 0;
            while (scanner.hasNext()) {
                OnDiskAtomIterator columnIterator = scanner.next();
                try {
                    // CFMetaData cfMetaData =
                    // Schema.instance.getCFMetaData(Descriptor.fromFilename(p.toString()));
                    // SSTableExport se = new SSTableExport();
                    // SSTableExport.export(Descriptor.fromFilename(p.toString()),
                    // System.out, new String[0]);
                    processRow(p, columnIterator, producer, tableName);

                } finally {
                    columnIterator.close();
                }
                long currentPosition = scanner.getCurrentPosition();
                if (bytesRead.addAndGet(currentPosition - previousPosition) > maxRead) {
                    return;
                }
                previousPosition = currentPosition;
            }
            start = System.nanoTime() - start;
        } finally {
            scanner.close();
        }
    }

    @SuppressWarnings("unused")
    protected void processRow(Path p, OnDiskAtomIterator columnIterator,
            EConsumer<AisPacket> producer, String tableName) throws Exception {
        while (columnIterator.hasNext()) {
            OnDiskAtom c = columnIterator.next();
            if (c instanceof Column) {
                Column col = (Column) c;
                if (col.name().hasRemaining()) {
                    AisPacket pa = AisPacket.fromByteBuffer(col.value());
                    producer.accept(pa);
                }
            }
        }
    }
}
