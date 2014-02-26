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
package dk.dma.ais.store.materialize.cli;


import dk.dma.ais.store.Archiver;
import dk.dma.ais.store.DriverFileExport;
import dk.dma.ais.store.FileExport;
import dk.dma.ais.store.FileImport;
import dk.dma.ais.store.materialize.jobs.AisStorePacketsTimeReadTest;
import dk.dma.ais.store.materialize.jobs.CountMMSIAis;
import dk.dma.commons.app.CliCommandList;

/**
 * The command line interface to AisMat, baed on Main from dk.dma.ais.Store.
 * 
 * @author Jens Tuxen
 */
public class Main {

    public static void main(String[] args) throws Exception {
        CliCommandList c = new CliCommandList("AisStore");
        c.add(Archiver.class, "archive", "Reads data from AIS datasources and stores data into Cassandra");
        c.add(FileImport.class, "import", "Imports data from text files and stores data into Cassandra");
        c.add(FileExport.class, "export", "Exports data from Cassandra into text files");
        c.add(DriverFileExport.class, "driverexport", "Exports data using the datastax driver");
        
        //ais-store-materialize
        c.add(CountMMSIAis.class, "mmsi_time_view", "((mmsi,time),count) materialized view");
        c.add(AisStorePacketsTimeReadTest.class, "time_read_test", "test packets_time read speed of aisstore");
        c.invoke(args);
    }
}
