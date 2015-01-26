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
package dk.dma.ais.store.importer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

/**
 * Creates an AisStore Table/Schema writer, see AisStoreTableWriters for implementation
 * @param types note: need to be aware of super composite keys as partition key, for instance.
 * @author Jens Tuxen
 *
 */
public class AisStoreTableWriter implements AisStoreRowGenerator {

    private Murmur3Partitioner partitioner = new Murmur3Partitioner();
    private CompositeType cmpType;
    
    SSTableSimpleUnsortedWriter writer;
    AbstractType<?>[] types;
    ByteBuffer[] keys;
    
    int chunkLength = 256;


    public final SSTableSimpleUnsortedWriter getWriter() {
        return writer;
    }

    public AisStoreTableWriter(String inDirectory,String keyspace,String tableName,Collection<String>keyNames, int chunkLength,AbstractType<?>... types) throws ConfigurationException {
        cmpType = CompositeType.getInstance(types);
        keys = keyNames.stream().map(keyName -> cmpType.builder().add((ByteBuffer.wrap(keyName.getBytes()))).build()).collect(Collectors.toList()).toArray(new ByteBuffer[0]);
        this.chunkLength = chunkLength;
        CompressionParameters compOpts = new CompressionParameters("LZ4Compressor", chunkLength, new HashMap<String,String>());
        writer = new SSTableSimpleUnsortedWriter(Paths.get(inDirectory, "/"+tableName).toFile(), partitioner, keyspace, tableName, cmpType, null,128, compOpts);
        
    }
    
    public void addRow(ByteBuffer[] values, long timestamp) throws IOException {        
        writer.newRow(values[0]);
        for (int i=1; i<values.length; i++) {
            writer.addColumn(keys[i], cmpType.builder().add(values[i]).build(), timestamp);
        }
        
    }
    
    public void close() throws IOException {
        writer.close();
    }

}
 
