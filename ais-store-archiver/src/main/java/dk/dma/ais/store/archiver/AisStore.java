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

import java.util.ArrayList;
import java.util.Arrays;

/**
 * The main entry point to the AisStore functionality.
 * 
 * @author Kasper Nielsen
 */
public class AisStore {

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
