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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import dk.dma.ais.reader.AisReader;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class CassandraWriteSink<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AisReader.class);

    private final AtomicLong elementWritten = new AtomicLong();

    public abstract void process(MutationBatch b, T message);

    public void onSucces(List<T> messages) {
        elementWritten.addAndGet(messages.size());
    }

    public void onFailure(List<T> messages, ConnectionException cause) {
        LOG.error("Could not persist messages in cassandra", cause);
    }
}
