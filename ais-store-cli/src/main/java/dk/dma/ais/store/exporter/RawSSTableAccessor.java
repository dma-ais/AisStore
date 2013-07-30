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
package dk.dma.ais.store.exporter;

import static dk.dma.ais.store.AisStoreSchema.TABLE_TIME;
import static java.util.Objects.requireNonNull;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.naming.ConfigurationException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

import dk.dma.ais.packet.AisPacket;
import dk.dma.commons.util.FormatUtil;
import dk.dma.enav.util.function.EConsumer;

/**
 * 
 * @author Kasper Nielsen
 */
public class RawSSTableAccessor {
    /** The logger. */
    static final Logger LOG = LoggerFactory.getLogger(RawSSTableAccessor.class);

    /** The name of the keyspace to use. */
    private final String keyspace;

    /** The URL to cassandra.yaml */
    private final String yamlUrl;

    /** The number of bytes that have been read from disk. */
    final AtomicLong bytesRead = new AtomicLong();

    public RawSSTableAccessor(String yamlUrl, String keyspace) {
        this.yamlUrl = requireNonNull(yamlUrl);
        this.keyspace = requireNonNull(keyspace);
    }

    public void process(Interval interval, EConsumer<AisPacket> consumer) throws Exception {
        System.setProperty("cassandra.config", "file://" + yamlUrl);

        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemTables().size() < 1) {
            throw new ConfigurationException("no non-system tables are defined");
        }

        // Create a new immutable snapshot that we will operate on. This will also flush all data to disk
        String snapshotName = new Date().toString().replace(' ', '_').replace(':', '_');
        CassandraNodeTool node = new CassandraNodeTool();
        node.takeSnapshot(keyspace, snapshotName);
        LOG.info("Snapshot of " + keyspace + " made to " + snapshotName);
        // Start processing all SSTables

        long start = System.nanoTime();
        try {
            Exception failure = null;
            try {
                processDataFileLocations(snapshotName, consumer);
            } catch (Exception e) {
                failure = e;
            } finally {
                start = System.nanoTime() - start;
                long s = TimeUnit.NANOSECONDS.toSeconds(start);
                double minuteAvg = bytesRead.get() * ((double) TimeUnit.MINUTES.toNanos(1) / (double) start);

                System.out.println("Read a total of " + FormatUtil.humanReadableByteCount(bytesRead.get(), true)
                        + " in " + String.format("%d:%02d:%02d", s / 3600, s % 3600 / 60, s % 60) + " ("
                        + FormatUtil.humanReadableByteCount((long) minuteAvg, true) + "/min)");
                if (failure != null) {
                    throw failure;
                }
            }
        } finally {
            node.deleteSnapshot(keyspace, snapshotName);
        }
    }

    protected void processDataFileLocations(String snapshotName, EConsumer<AisPacket> producer) throws Exception {
        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            Path snapshots = Paths.get(s).resolve(keyspace).resolve(TABLE_TIME).resolve("snapshots")
                    .resolve(snapshotName);
            // iterable through all data files (xxxx-Data)
            // if the dataformat changes hf needs to be upgraded to the current versino
            // http://svn.apache.org/repos/asf/cassandra/trunk/src/java/org/apache/cassandra/io/sstable/Descriptor.java
            System.out.println(s);
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(snapshots, keyspace + "-" + TABLE_TIME
                    + "-ic-*-Data.db")) {
                for (Path p : ds) { // for each data file
                    // System.out.println(p);
                    processDataFile(p, producer);
                }
            }
        }
    }

    protected void processDataFile(Path p, EConsumer<AisPacket> producer) throws Exception {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(p.toString()));
        SSTableScanner scanner = reader.getDirectScanner(RateLimiter.create(Double.MAX_VALUE));
        long sizeOnDisk = scanner.getLengthInBytes();
        System.out.println("Processing " + p + " [size = " + sizeOnDisk + ", uncompressed = "
                + reader.uncompressedLength() + "]");
        long start = System.nanoTime();
        try {
            long previousPosition = 0;
            while (scanner.hasNext()) {
                OnDiskAtomIterator columnIterator = scanner.next();
                try {
                    System.out.println(columnIterator);
                    // processRow(p, columnIterator, producer);
                } finally {
                    columnIterator.close();
                }
                long currentPosition = scanner.getCurrentPosition();
                bytesRead.addAndGet(currentPosition - previousPosition);
                previousPosition = currentPosition;
            }
            start = System.nanoTime() - start;
        } finally {
            scanner.close();
        }
    }

}
