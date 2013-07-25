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

import static java.util.Objects.requireNonNull;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

import com.google.common.util.concurrent.RateLimiter;

import dk.dma.ais.packet.AisPacket;
import dk.dma.enav.util.function.EConsumer;

/**
 * 
 * @author Kasper Nielsen
 */
class SSTableProcessor implements Callable<Void> {
    final AtomicLong bytesRead;
    final Path p;
    volatile long processingTime;
    @SuppressWarnings("rawtypes")
    final Range<Token> range;
    final EConsumer<AisPacket> rowProcessor;

    @SuppressWarnings("rawtypes")
    SSTableProcessor(Path p, AtomicLong bytesRead, long maxRead, EConsumer<AisPacket> rowProcessor, Range<Token> range) {
        this.p = requireNonNull(p);
        this.bytesRead = requireNonNull(bytesRead);
        this.rowProcessor = requireNonNull(rowProcessor);
        this.range = requireNonNull(range);
    }

    public Void call() throws Exception {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(p.toString()));
        SSTableScanner scanner = reader.getDirectScanner(RateLimiter.create(Double.MAX_VALUE));
        // ICompactionScanner scanner = reader.getDirectScanner(range);
        long sizeOnDisk = scanner.getLengthInBytes();
        System.out.println("Processing " + p + " [size = " + sizeOnDisk + ", uncompressed = "
                + reader.uncompressedLength() + "]");
        long start = System.nanoTime();
        try {
            long previousPosition = 0;
            scanner.seekTo(null);
            while (scanner.hasNext()) {
                OnDiskAtomIterator columnIterator = scanner.next();
                try {
                    // columnIterator.
                    // while(columnIterator.hasNext()) {
                    // columnIterator.next();
                    // }
                    // columnIterator.
                    // rowProcessor.process(columnIterator);
                } finally {
                    columnIterator.close();
                }
                long currentPosition = scanner.getCurrentPosition();
                bytesRead.addAndGet(currentPosition - previousPosition);
                previousPosition = currentPosition;
            }
            processingTime = System.nanoTime() - start;
        } finally {
            scanner.close();
        }
        return null;
    }
}
