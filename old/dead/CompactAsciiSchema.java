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
package dk.dma.ais.store.cassandra.schema;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;

import dk.dma.ais.packet.AisPacket;
import dk.dma.app.cassandra.CassandraWriteSink;

/**
 * 
 * @author Kasper Nielsen
 */
public class CompactAsciiSchema extends CassandraWriteSink<AisPacket> {

    public final static String TABLE = "ais_baseline_archive";

    public final static ColumnFamily<byte[], String> CF = new ColumnFamily<>(TABLE, BytesArraySerializer.get(),
            StringSerializer.get());

    public static final CompactAsciiSchema INSTANCE = new CompactAsciiSchema();

    /** {@inheritDoc} */
    @Override
    public void process(MutationBatch b, AisPacket message) {
        long l = message.getTimestamp().getTime();
        ColumnListMutation<String> r = b.withRow(CF, Bytes.concat(Longs.toByteArray(l), message.calculateHash128()));
        // r.putColumn("m", "A");
        r.putColumn("m", message.getStringMessage());
    }
}
