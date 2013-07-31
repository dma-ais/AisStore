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
package dk.dma.ais.store.raw.job;

import java.io.Serializable;

import com.google.common.base.Supplier;

import dk.dma.ais.packet.AisPacket;
import dk.dma.enav.util.function.Consumer;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class ReducingJob implements Consumer<AisPacket>, Serializable {

    /** serialVersionUID. */
    private static final long serialVersionUID = 1L;

    protected abstract ReducingJob combine(ReducingJob other);
}

// <T extends Consumer<E>> CollectionView<T> gather(Supplier<T> gatherer);

abstract class AbstractJob<T> implements Supplier<Consumer<AisPacket>> {

    // AbstractJob<T> reduceWith
}

@SuppressWarnings("serial")
class MessageCount extends ReducingJob {
    long count;

    public MessageCount(long count) {
        this.count = count;
    }

    /** {@inheritDoc} */
    @Override
    public void accept(AisPacket t) {
        count++;
    }

    /** {@inheritDoc} */
    @Override
    protected ReducingJob combine(ReducingJob other) {
        return null;
    }
}
