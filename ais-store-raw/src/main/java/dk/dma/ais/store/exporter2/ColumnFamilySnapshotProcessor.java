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
package dk.dma.ais.store.exporter2;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;

import com.google.common.base.Function;

import dk.dma.enav.util.function.EConsumer;

/**
 * 
 * @author Kasper Nielsen
 */
public class ColumnFamilySnapshotProcessor implements Closeable {

    /** The number of bytes that have been read from disk. */
    final AtomicLong bytesRead = new AtomicLong();

    /** The name of the keyspace to use. */
    final String keyspace;

    /** The name of the column family to use. */
    final String columnFamily;

    /** The maximum number of bytes we want to read, useful for tests. */
    final long maxRead;

    /** The URL to cassandra.yaml */
    final String yamlUrl;

    public ColumnFamilySnapshotProcessor(String yamlUrl, String keyspace, String columnFamily, long maxRead) {
        this.yamlUrl = requireNonNull(yamlUrl);
        this.keyspace = requireNonNull(keyspace);
        this.columnFamily = requireNonNull(columnFamily);
        this.maxRead = maxRead;
    }

    public static ColumnFamilySnapshotProcessor create(String yamlUrl, String keyspace, String columnFamily) {
        return create(yamlUrl, keyspace, columnFamily, Long.MAX_VALUE);
    }

    public static ColumnFamilySnapshotProcessor create(String yamlUrl, String keyspace, String columnFamily,
            long maxRead) {
        return new ColumnFamilySnapshotProcessor(yamlUrl, keyspace, columnFamily, maxRead);
    }

    public <T> Job processAll(Function<OnDiskAtomIterator, T> mapper, BlockingQueue<T> queue) {
        return null;
    }

    public Job processAll(EConsumer<OnDiskAtomIterator> processor) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {}

    public class Job extends FutureTask<Void> {

        /**
         * @param runnable
         * @param result
         */
        Job(Runnable runnable, Void result) {
            super(runnable, result);
        }

    }
}
