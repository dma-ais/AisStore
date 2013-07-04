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
