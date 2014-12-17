/* Copyright (c) 2011 Danish Maritime Authority.
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
package dk.dma.ais.store.raw.job;

import java.io.Serializable;
import java.util.function.Consumer;

import com.google.common.base.Supplier;

import dk.dma.ais.packet.AisPacket;

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
        return new MessageCount(count + ((MessageCount) other).count);
    }
}
