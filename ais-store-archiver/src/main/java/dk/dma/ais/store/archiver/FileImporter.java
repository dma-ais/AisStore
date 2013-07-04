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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import dk.dma.ais.packet.AisPacket;

/**
 * 
 * @author Kasper Nielsen
 */
class FileImporter {
    private static final int SIZE = 10 * Store.BATCH_SIZE;

    /** Where files should be read from. */
    Path readFrom;

    /** Only files with the specified suffix will be read. */
    String suffix;

    /** Where files should be moved to after having been processed. */
    Path moveTo;

    ArrayList<AisPacket> outstanding = new ArrayList<>(SIZE);

    @SuppressWarnings("unused")
    void readFile(Path p) throws IOException {
        // laes 1000 ting, indsaat dem
    }

}
