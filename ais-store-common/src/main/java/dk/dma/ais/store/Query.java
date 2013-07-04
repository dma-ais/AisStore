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
package dk.dma.ais.store;

import static java.util.Objects.requireNonNull;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.enav.util.function.EBlock;
import dk.dma.enav.util.function.ESupplier;
import dk.dma.enav.util.function.Function;
import dk.dma.enav.util.function.Predicate;

/**
 * A
 * 
 * @author Kasper Nielsen
 */
public abstract class Query<T> implements Iterable<T> {

    public static <T> Query<T> emptyQuery() {
        return new Query<T>() {
            protected ESupplier<T> createSupplier() throws Exception {
                return new ESupplier<T>() {
                    public T get() throws Exception {
                        return null;
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<T> iterator() {
        try {
            return getResultsAsList().iterator();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public final Query<T> filter(final Predicate<? super T> predicate) {
        requireNonNull(predicate);
        return new InternalQuery<T>() {

            /** {@inheritDoc} */
            @Override
            Future<Void> streamResults(final Callback<T> callback, long limit) {
                return Query.this.streamResults(new Callback<T>() {
                    public void start() throws Exception {
                        callback.start();
                    }

                    public void stop() throws Exception {
                        callback.stop();
                    }

                    @Override
                    public void accept(T t) throws Exception {
                        if (predicate.test(t)) {
                            callback.accept(t);
                        }
                    }
                }, limit);
            }
        };
    }

    /**
     * Returns the result as a list.
     * 
     * @return the result as a list
     */
    public final List<T> getResultsAsList() throws Exception {
        final ArrayList<T> result = new ArrayList<>();
        ESupplier<T> s = createSupplier();
        for (T t = s.get(); t != null; t = s.get()) {
            result.add(t);
        }
        return result;
    }

    public final <R> Query<R> map(final Function<? super T, ? extends R> mapper) {
        requireNonNull(mapper);
        return new InternalQuery<R>() {

            /** {@inheritDoc} */
            @Override
            Future<Void> streamResults(final Callback<R> callback, long limit) {
                return Query.this.streamResults(new Callback<T>() {
                    public void start() throws Exception {
                        callback.start();
                    }

                    public void stop() throws Exception {
                        callback.stop();
                    }

                    @Override
                    public void accept(T t) throws Exception {
                        callback.accept(mapper.apply(t));
                    }
                }, limit);
            }
        };
    }

    protected abstract ESupplier<T> createSupplier() throws Exception;

    /**
     * Streams the result to the callback.
     * 
     * @param callback
     *            the callback to stream the result to
     * @return a future that returns when the query has finished
     */
    Future<Void> streamResults(final Callback<T> callback, final long limit) {
        requireNonNull(callback);
        Callable<Void> c = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ESupplier<T> s = createSupplier();
                callback.start();
                try {
                    for (T t = s.get(); t != null; t = s.get()) {
                        callback.accept(t);
                    }
                } finally {
                    callback.stop();
                }
                return null;
            }
        };
        FutureTask<Void> task = new FutureTask<>(c);
        submit(task);
        return task;
    }

    public final Future<Void> streamResults(final OutputStream stream, final OutputStreamSink<T> sink, long limit) {
        requireNonNull(stream);
        requireNonNull(sink);
        return streamResults(new Callback<T>() {
            public void start() throws Exception {
                sink.header(stream);
            }

            public void stop() throws Exception {
                sink.footer(stream);
            }

            @Override
            public void accept(T t) throws Exception {
                sink.process(stream, t);
            }
        }, limit);
    }

    public final Future<Void> streamResults(final OutputStream stream, final OutputStreamSink<T> sink) {
        return streamResults(stream, sink, Long.MAX_VALUE);
    }

    /** {@inheritDoc} */
    protected void submit(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.setName("QueryThread[ + " + toString() + "]");
        t.start();
    }

    abstract static class InternalQuery<T> extends Query<T> {
        protected ESupplier<T> createSupplier() {
            throw new Error();// Should never been classed
        }

        protected void submit(Runnable runnable) {
            throw new Error();// Should never been classed
        }
    }

    abstract static class Callback<T> extends EBlock<T> {
        abstract void start() throws Exception;

        abstract void stop() throws Exception;
    }
}
