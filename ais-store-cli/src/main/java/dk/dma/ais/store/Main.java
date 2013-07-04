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

import java.util.ArrayList;
import java.util.Arrays;

import dk.dma.ais.store.archiver.FileExport;
import dk.dma.ais.store.archiver.FileImport;
import dk.dma.ais.store.archiver.FileStore;
import dk.dma.ais.store.archiver.Store;

/**
 * The main entry point to the AisStore functionality.
 * 
 * @author Kasper Nielsen
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // Commands with different options are not supported by JCommander
        // So we have to write some custom code.
        ArrayList<String> list = new ArrayList<>(Arrays.asList(args));
        int cmdIndex = 0;
        for (;; cmdIndex++) {
            if (cmdIndex == list.size()) {
                printError("No command specified");
            } else if (!list.get(cmdIndex).startsWith("-")) {
                break;
            }
        }
        String command = list.get(cmdIndex);
        list.remove(cmdIndex);
        args = list.toArray(new String[list.size()]);
        if (command.equals("store")) {
            Store.main(args);
        } else if (command.equals("fileimport")) {
            FileImport.main(args);
        } else if (command.equals("fileexport")) {
            FileExport.main(args);
        } else if (command.equals("filestore")) {
            FileStore.main(args);
        } else {
            printError("Unknown command specified: " + command);
        }
    }

    static void printError(String errorMessage) {
        System.out.println(errorMessage);
        System.out.println("The available AisStore commands are:");
        System.out.println("    store        Reads data from datasources and stores data into Cassandra");
        System.out.println("    fileimport   Reads data from files and stores data into Cassandra");
        System.out.println("    fileexport   Exports data from Cassandra into files");
        System.out.println("    filestore    Reads data from datasources and stores data into files");
        System.exit(1);
    }
}
