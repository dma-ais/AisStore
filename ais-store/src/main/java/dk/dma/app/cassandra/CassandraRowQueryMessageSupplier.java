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

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.query.RowQuery;

import dk.dma.ais.store.query.Query;
import dk.dma.enav.util.function.EFunction;
import dk.dma.enav.util.function.ESupplier;

/**
 * 
 * @author Kasper Nielsen
 */
public class CassandraRowQueryMessageSupplier<T, K, C> extends Query<T> {

    final ColumnFamily<K, C> cf;
    final KeySpaceConnection connection;
    final EFunction<Column<C>, T> function;
    final K key;

    final int LIMIT = 5000;
    final C start;

    final C stop;

    public CassandraRowQueryMessageSupplier(KeySpaceConnection connection, ColumnFamily<K, C> cf, K key,
            EFunction<Column<C>, T> function, C start, C stop) {
        this.connection = requireNonNull(connection);
        this.key = requireNonNull(key);
        this.cf = requireNonNull(cf);
        this.function = requireNonNull(function);
        this.start = start;
        this.stop = stop;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws ConnectionException
     */
    @Override
    protected ESupplier<T> createSupplier() throws ConnectionException {

        return new ESupplier<T>() {
            int count;
            Column<C> last;
            RowQuery<K, C> q = connection.prepareQuery(cf).getKey(key).autoPaginate(true)
                    .withColumnRange(start, stop, false, LIMIT);
            Iterator<Column<C>> i = q.execute().getResult().iterator();

            @Override
            public T get() throws Exception {
                if (count == LIMIT) {
                    count = 0;
                    q = connection.prepareQuery(cf).getKey(key).withColumnRange(last.getName(), stop, false, LIMIT);
                    i = q.execute().getResult().iterator();
                    // Since it is not possible to exclude the first element we need to do it ourself
                    if (i.hasNext()) {
                        i.next(); // We last (unfor
                        count++;
                    }
                    // System.out.println("Next Batch");
                }
                if (i.hasNext()) {
                    Column<C> c = last = i.next();
                    count++;
                    return function.apply(c);
                }
                return null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    protected void submit(Runnable runnable) {
        new Thread(runnable).start();
    }
}
