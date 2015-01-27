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
import java.util.Collection;

public interface AisStoreRowGenerator {
    /**
     * Column values to add + same timestamp for each
     * @param values
     * @param timestamp in milliseconds
     * @throws IOException 
     */
    public void addRow(ByteBuffer[] values, long timestamp) throws IOException;
}
