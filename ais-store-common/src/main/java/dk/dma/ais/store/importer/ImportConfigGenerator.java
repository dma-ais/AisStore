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
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportConfigGenerator {
    
    public static final void generate(String inDirectory) throws IOException, URISyntaxException {
        
        Path p = Paths.get(ImportConfigGenerator.class.getResource("/cassandra.yaml").toURI());

        Stream<String> r = Files.lines(p);
        
        Stream<String> result = r.map(line -> {
        
            if (line.contains("saved_caches_directory: PLACEHOLDER_SAVED_CACHES")) {
                return "saved_caches_directory: "+Paths.get(inDirectory, "/saved_caches").toAbsolutePath().toString();
            } else if (line.contains("commitlog_directory: PLACEHOLDER_COMMIT_LOG")) {
                return "commitlog_directory: "+Paths.get(inDirectory, "/commitlog").toAbsolutePath().toString();
            } else if (line.contains("data_file_directories: [PLACEHOLDER_DATA_FILE_DIRECTORIES]")) {
                return "data_file_directories: ["+Paths.get(inDirectory, "/data").toAbsolutePath().toString()+"]";
            }

            return line;
        });
        
        
        Files.write(Paths.get(inDirectory,"cassandra.yaml"), result.collect(Collectors.toList()), Charset.defaultCharset());
        
        
    }

}
