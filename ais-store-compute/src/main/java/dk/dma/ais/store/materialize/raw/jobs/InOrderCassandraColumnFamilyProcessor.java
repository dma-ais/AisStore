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

package dk.dma.ais.store.materialize.raw.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTableReader;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.old.exporter2.CompactionIterable;
import dk.dma.enav.util.function.EConsumer;

/**
 * @author Jens Tuxen
 *
 */
public class InOrderCassandraColumnFamilyProcessor extends
        SimpleCassandraColumnFamilyProcessor {
    
    /**
     * @param yamlUrl
     * @param keyspace
     */
    public InOrderCassandraColumnFamilyProcessor(String yamlUrl, String keyspace) {
        super(yamlUrl, keyspace);
    }

    /**
     * Process datafiles IN ORDER
     * @param snapshotName
     * @param consumer
     * @param tableName
     * @param gcBefore
     * @throws IOException
     */
    protected void processDataFileLocations(String snapshotName,EConsumer<AisPacket> consumer, String tableName, int gcBefore) throws IOException {
        Collection<SSTableReader> readers = getReaders(snapshotName,tableName);
        
        ArrayList<ICompactionScanner> scanners = new ArrayList<>();
        for (SSTableReader r: readers) {
            scanners.add(r.getScanner());
        }
        
        Keyspace keyspace = Keyspace.open("aisdata");
        ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(keyspace, tableName, false);
        CompactionController controller = new CompactionController(cfs,new HashSet<SSTableReader>(readers),gcBefore);
        
        CompactionIterable compactionIterable = new CompactionIterable(OperationType.COMPACTION, scanners, controller);
        for (AbstractCompactedRow row: compactionIterable) {
           
        }
        
    }

}
