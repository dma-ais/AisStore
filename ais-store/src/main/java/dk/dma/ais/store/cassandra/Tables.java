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
package dk.dma.ais.store.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * 
 * @author Kasper Nielsen
 */
public class Tables {

    public final static String AIS_MESSAGES = "aismessages";

    public final static ColumnFamily<byte[], String> AIS_MESSAGES_CF = new ColumnFamily<>(AIS_MESSAGES,
            BytesArraySerializer.get(), StringSerializer.get());

    public static final String AIS_MESSAGES_MESSAGE = "message";

    public final static ColumnFamily<Long, String> AIS_SHIPS = new ColumnFamily<>("aisships", LongSerializer.get(),
            StringSerializer.get());

    // Writer
    // QueryEngine (Online)
    // Extractor (tror vi replikere til en single host)
    // hvor vi traekker data ud
    // Saa goer det hele ikke noget den doer.

}
