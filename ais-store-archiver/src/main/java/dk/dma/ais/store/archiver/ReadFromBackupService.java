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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPackets;

/**
 * This class is responsible for reading backup files that are written when access to Cassandra is down.
 * 
 * 
 * @author Kasper Nielsen
 */
public class ReadFromBackupService extends AbstractExecutionThreadService {

    /** The archiver. */
    private final AisArchiver archiver;

    ReadFromBackupService(AisArchiver archiver) {
        this.archiver = requireNonNull(archiver);
    }

    /** {@inheritDoc} */
    @Override
    protected void run() throws Exception {
        // The backup directory
        Path backupDirectory = archiver.backup.toPath();

        // The current file we are trying to read into casssandra
        Path backupFile = null;

        // The packets we are currently trying to add into cassandra
        LinkedList<AisPacket> packets = new LinkedList<>();

        // Run in a loop until shutdown
        while (isRunning()) {
            Thread.sleep(1000); // Sleep for a bit

            try {
                // If we have no packets queued up. Let's see if there are files we can process
                if (packets.size() == 0) {
                    try (DirectoryStream<Path> ds = Files.newDirectoryStream(backupDirectory)) {
                        for (Path p : ds) {
                            // Add all the packets in the current file to packets
                            packets.addAll(AisPackets.readFromFile(p));
                            backupFile = p;
                            break;
                        }
                    }
                }

                // Keep reading packets from the list and add it to the cassandra queue (if it is not clogged)
                AisPacket p;
                while ((p = packets.peek()) != null) {
                    // We only add elements if the queue is not to clogged
                    if (archiver.getNumberOfOutstandingPackets() > 10 * AisArchiver.BATCH_SIZE
                            || !archiver.mainStage.getInputQueue().offer(p)) {
                        break;// Lets sleep a little before adding more packets to the queue
                    }
                    packets.remove();// remove the peaked element
                }

                // If all packets have been processed from the current backup file. Delete it
                if (packets.size() == 0 && backupFile != null) {
                    try {
                        Files.delete(backupFile);// empty file
                    } catch (IOException e) {
                        // Okay not great. Because we will keep reading the same file
                    }
                    backupFile = null; // file has been deleted
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
