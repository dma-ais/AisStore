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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.columniterator.IColumnIterator;

import com.google.common.base.Function;

import dk.dma.app.util.function.EBlock;

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

    public <T> Job processAll(Function<IColumnIterator, T> mapper, BlockingQueue<T> queue) {
        return null;
    }

    public Job processAll(EBlock<IColumnIterator> processor) {
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
