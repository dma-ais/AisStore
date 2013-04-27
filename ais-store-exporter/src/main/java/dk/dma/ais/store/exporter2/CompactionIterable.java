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
package dk.dma.ais.store.exporter2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.AbstractCompactionIterable;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class CompactionIterable extends AbstractCompactionIterable {
    // private static Logger logger = LoggerFactory.getLogger(CompactionIterable.class);

    private long row;

    private static final Comparator<OnDiskAtomIterator> comparator = new Comparator<OnDiskAtomIterator>() {
        public int compare(OnDiskAtomIterator i1, OnDiskAtomIterator i2) {
            return i1.getKey().compareTo(i2.getKey());
        }
    };

    public CompactionIterable(OperationType type, List<ICompactionScanner> scanners, CompactionController controller) {
        super(controller, type, scanners);
        row = 0;
    }

    public CloseableIterator<AbstractCompactedRow> iterator() {
        return MergeIterator.get(scanners, comparator, new Reducer());
    }

    public String toString() {
        return this.getCompactionInfo().toString();
    }

    protected class Reducer extends MergeIterator.Reducer<OnDiskAtomIterator, AbstractCompactedRow> {
        protected final List<SSTableIdentityIterator> rows = new ArrayList<>();

        public void reduce(OnDiskAtomIterator current) {
            rows.add((SSTableIdentityIterator) current);
        }

        @SuppressWarnings("synthetic-access")
        protected AbstractCompactedRow getReduced() {
            assert !rows.isEmpty();

            try {
                AbstractCompactedRow compactedRow = controller.getCompactedRow(new ArrayList<>(rows));
                if (compactedRow.isEmpty()) {
                    controller.invalidateCachedRow(compactedRow.key);
                    return null;
                }

                // If the row is cached, we call removeDeleted on at read time it to have coherent query returns,
                // but if the row is not pushed out of the cache, obsolete tombstones will persist indefinitely.
                controller.removeDeletedInCache(compactedRow.key);
                return compactedRow;
            } finally {
                rows.clear();
                if (row++ % 1000 == 0) {
                    long n = 0;
                    for (ICompactionScanner scanner : scanners) {
                        n += scanner.getCurrentPosition();
                    }
                    bytesRead = n;
                    controller.mayThrottle(bytesRead);
                }
            }
        }
    }
}
