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
package dk.dma.ais.store.cassandra.support;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class CassandraWriteSink<T> {

    private final AtomicLong elementWritten = new AtomicLong();

    public abstract void process(MutationBatch b, T message);

    public void onSucces(List<T> messages) {
        elementWritten.addAndGet(messages.size());
    }

    public abstract void onFailure(List<T> messages, ConnectionException cause);
}
