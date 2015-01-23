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
package dk.dma.ais.store.old.exporter3;

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
