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

package dk.dma.ais.store.materialize.triggers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Random;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.triggers.ITrigger;

/**
 * @author Jens Tuxen
 * 
 */
public class PacketsMMSITrigger implements ITrigger {

    @Override
    public Collection<RowMutation> augment(ByteBuffer key, ColumnFamily update) {
        return null;
    }

    /**
     * Helper function that gets the value of a ByteBuffer (key or column) using
     * db.marshal
     * 
     * @param bytes
     * @param marshalType
     * @return string representation of the value
     */
    protected static String getAsString(java.nio.ByteBuffer bytes,
            String marshalType) {

        String val = null;
        try {
            AbstractType<?> abstractType = TypeParser.parse(marshalType);
            val = abstractType.getString(bytes);
        } catch (ConfigurationException | SyntaxException e) {
            e.printStackTrace();
        }

        return val;
    }

    /**
     * 
     * @param s
     * @return
     */
    private static ByteBuffer getStringAsByteBuffer(String s) {
        ByteBuffer bf = Charset.forName("UTF-8").encode(s);
        return bf;
    }

}
