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

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;

/**
 * 
 * @author Kasper Nielsen
 */
public class FileExport {

    @Parameter(names = "-backup", description = "The backup directory")
    File backup = new File("aisbackup");

    @Parameter(names = "-database", description = "The cassandra database to export data from")
    String cassandraDatabase = "aisdata";

    @Parameter(names = "-database", description = "The directory where cassandra stores A list of cassandra hosts that data can be exported from")
    List<String> cassandraSeeds = Arrays.asList("localhost");

    @Parameter(names = "-sourceFilter", description = "The sourceFilter to apply")
    String sourceFilter;

    @Parameter(names = "-interval", description = "The ISO 8601 time interval for data to export")
    String interval;

    /**
     * @param args
     */
    public static void main(String[] args) {}

}
