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
package dk.dma.ais.store.archiver;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import dk.dma.ais.reader.AisTcpReader;
import dk.dma.commons.management.ManagedOperation;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisConnectionManager {

    /** A map of all currently managed readers */
    final ConcurrentHashMap<String, AisTcpReader> readers = new ConcurrentHashMap<>();

    public synchronized void addSource(List<String> sources) {

    }

    public synchronized void addSource(String name, String value) {
        requireNonNull(name, "name is null");
        requireNonNull(name, "value is null");
        name = name.toLowerCase(); // always use lowercase
        if (readers.containsKey(name)) {
            throw new IllegalArgumentException("A source with the specified already exists, name = " + name);
        }
    }

    @ManagedOperation
    public Set<String> getSourceNames() {
        return new TreeSet<>(readers.keySet());
    }
}
