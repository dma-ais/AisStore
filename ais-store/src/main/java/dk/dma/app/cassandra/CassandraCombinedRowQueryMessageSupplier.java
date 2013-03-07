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
package dk.dma.app.cassandra;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import dk.dma.ais.store.query.Query;
import dk.dma.enav.util.function.ESupplier;

/**
 * 
 * @author Kasper Nielsen
 */
public class CassandraCombinedRowQueryMessageSupplier<T, K, C> extends Query<T> {
    final Iterator<CassandraRowQueryMessageSupplier<T, K, C>> queries;

    final AtomicLong resultCount = new AtomicLong();

    /**
     * @param queries
     */
    public CassandraCombinedRowQueryMessageSupplier(Collection<CassandraRowQueryMessageSupplier<T, K, C>> queries) {
        this.queries = queries.iterator();
    }

    /** {@inheritDoc} */
    @Override
    protected ESupplier<T> createSupplier() throws Exception {
        return new ESupplier<T>() {
            CassandraRowQueryMessageSupplier<T, K, C> current = queries.hasNext() ? queries.next() : null;
            ESupplier<T> supplier;

            @Override
            public T get() throws Exception {
                // if (resultCount.get() % 1000 == 0) {
                // System.out.println(resultCount);
                // }
                while (current != null) {
                    if (supplier == null) {
                        supplier = current.createSupplier();
                        // System.out.println("Created new supplier");
                    }
                    T result = supplier.get();
                    if (result != null) {
                        resultCount.incrementAndGet();
                        return result;
                    }
                    supplier = null;
                    current = queries.hasNext() ? queries.next() : null;
                }
                // System.out.println("DONE");
                return null;
            }
        };
    }
}
