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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import dk.dma.ais.packet.AisPacket;

/**
 * 
 * @author Kasper Nielsen
 */
class FileImporter {
    private final static int SIZE = 10 * Store.BATCH_SIZE;

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
