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

/**
 * @author Jens Tuxen
 */
package dk.dma.ais.store.materialize.triggers;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.triggers.ITrigger;


/**
 * @author Jens Tuxen
 *
 */
public class TimeBlockTrigger implements ITrigger {

    /* (non-Javadoc)
     * @see org.apache.cassandra.triggers.ITrigger#augment(java.nio.ByteBuffer, org.apache.cassandra.db.ColumnFamily)
     */
    @Override
    public Collection<RowMutation> augment(ByteBuffer arg0, ColumnFamily arg1) {
        // TODO Auto-generated method stub
        return null;
    }

}
