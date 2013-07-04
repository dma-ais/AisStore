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
 * This class is responsible for reading text based ais files.
 * 
 * 
 * @author Kasper Nielsen
 */
public class FileImport extends AbstractExecutionThreadService {

    /** The archiver. */
    private final Archiver archiver;

    FileImport(Archiver archiver) {
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
            if (Files.exists(backupDirectory)) {
                try {
                    // If we have no packets queued up. Let's see if there are files we can process
                    if (packets.size() == 0) {
                        try (DirectoryStream<Path> ds = Files.newDirectoryStream(backupDirectory)) {
                            for (Path p : ds) {
                                // Add all the packets in the current file to packets
                                // Vi kan potentielt have meget store filer. Vi bliver noedt til at
                                // streame fra filer istedet for
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
                        if (archiver.getNumberOfOutstandingPackets() > 10 * Archiver.BATCH_SIZE
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

    /**
     * @param args
     */
    public static void main(String[] args) {}
}
