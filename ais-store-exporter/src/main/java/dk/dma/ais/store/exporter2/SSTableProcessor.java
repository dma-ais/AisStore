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
package dk.dma.ais.store.exporter2;

import static java.util.Objects.requireNonNull;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

/**
 * 
 * @author Kasper Nielsen
 */
class SSTableProcessor implements Callable<Void> {
    final AtomicLong bytesRead;
    final long maxRead;
    final Path p;
    volatile long processingTime;
    @SuppressWarnings("rawtypes")
    final Range<Token> range;
    final RowProcessor rowProcessor;

    @SuppressWarnings("rawtypes")
    SSTableProcessor(Path p, AtomicLong bytesRead, long maxRead, RowProcessor rowProcessor, Range<Token> range) {
        this.p = requireNonNull(p);
        this.bytesRead = requireNonNull(bytesRead);
        this.maxRead = maxRead;
        this.rowProcessor = requireNonNull(rowProcessor);
        this.range = requireNonNull(range);
    }

    public Void call() throws Exception {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(p.toString()));
        SSTableScanner scanner = reader.getDirectScanner(range);
        long sizeOnDisk = scanner.getLengthInBytes();
        System.out.println("Processing " + p + " [size = " + sizeOnDisk + ", uncompressed = "
                + reader.uncompressedLength() + "]");
        long start = System.nanoTime();
        try {
            long previousPosition = 0;
            while (scanner.hasNext()) {
                IColumnIterator columnIterator = scanner.next();
                try {
                    rowProcessor.process(columnIterator);
                } finally {
                    columnIterator.close();
                }
                long currentPosition = scanner.getCurrentPosition();
                if (bytesRead.addAndGet(currentPosition - previousPosition) > maxRead) {
                    return null;
                }
                previousPosition = currentPosition;
            }
            processingTime = System.nanoTime() - start;
        } finally {
            scanner.close();
        }
        return null;
    }
}
