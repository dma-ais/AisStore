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
package dk.dma.ais.store;

import dk.dma.commons.app.CliCommandList;

/**
 * The command line interface to AisStore.
 * 
 * @author Kasper Nielsen
 */
public class Main {

    public static void main(String[] args) throws Exception {
        CliCommandList c = new CliCommandList("AisStore");
        c.add(Archiver.class, "archive", "Reads data from AIS datasources and stores data into Cassandra");
        c.add(FileImport.class, "import", "Imports data from text files and stores data into Cassandra");
        c.add(FileExport.class, "export", "Exports data from Cassandra into text files");
        c.invoke(args);
    }
}
