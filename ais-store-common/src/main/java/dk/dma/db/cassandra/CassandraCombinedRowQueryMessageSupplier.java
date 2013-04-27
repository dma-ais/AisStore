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
package dk.dma.db.cassandra;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import dk.dma.db.Query;
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
