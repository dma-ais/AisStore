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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketInputStream;
import dk.dma.commons.util.io.PathUtil;

/**
 * This class is responsible for reading text based ais files.
 * 
 * 
 * @author Kasper Nielsen
 */
public class FileImport extends AbstractExecutionThreadService {

    /** The logger. */
    private static final Logger LOG = LoggerFactory.getLogger(FileImport.class);

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
        LOG.info("Using " + backupDirectory.toAbsolutePath() + " for backup");
        try {
            Files.createDirectories(backupDirectory);
        } catch (IOException e) {
            LOG.error("Could not create backup directory, exiting", e);
            System.exit(1);
        }
        // Run in a loop until shutdown
        while (isRunning()) {
            archiver.sleepUnlessShutdown(1, TimeUnit.SECONDS);

            // only start reading backups if there is no pressure on cassandra
            if (archiver.getNumberOfOutstandingPackets() < 5 * Archiver.BATCH_SIZE) {
                if (Files.exists(backupDirectory)) {
                    try {
                        // Let's see if there are files we can process
                        try (DirectoryStream<Path> ds = Files.newDirectoryStream(backupDirectory)) {
                            for (Path p : ds) {
                                if (p.getFileName().toString().endsWith(".zip")) {
                                    try {
                                        restoreFile(p);
                                    } catch (Exception e) {
                                        LOG.error("Unknown error while trying to restore backup from file " + p, e);
                                        Path ne = PathUtil.findUnique(p.resolveSibling(p.getFileName().toString()
                                                + ".unreadable"));
                                        LOG.error("Trying to rename the file to " + ne, e);
                                        try {
                                            Files.move(p, ne);
                                        } catch (IOException ioe) {
                                            LOG.error("Could not rename file ", ioe);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Unknown error while trying to restore backup ", e);
                    }
                }
            }
        }
    }

    private void restoreFile(Path p) throws IOException, InterruptedException {
        LOG.info("Trying to restore " + p);
        try (AisPacketInputStream s = AisPacketInputStream.createFromFile(p, true)) {
            AisPacket packet = null;
            while ((packet = s.readPacket()) != null) {
                // we might be overloaded so sleep for a bit if we cannot write the packet
                while (isRunning()) {
                    int q = archiver.getNumberOfOutstandingPackets();
                    if (q > 10 * Archiver.BATCH_SIZE) {
                        LOG.info("Write queue to Cassandra is to busy size=" + q + ", sleeping for a bit");
                    } else if (archiver.mainStage.getInputQueue().offer(packet)) {
                        System.out.println("Got it");
                        break;
                    } else {
                        LOG.info("Write queue to Cassandra was full size=" + q + ", sleeping for a bit");
                    }
                    archiver.sleepUnlessShutdown(1, TimeUnit.SECONDS);
                }
                // System.out.println("Packet added");
                if (!isRunning()) {
                    return;
                }
            }
        }
        LOG.info("Finished restoring " + p);
        try {
            Files.delete(p);// empty file
        } catch (IOException e) {
            LOG.error("Could not delete backup file: " + p, e);// Auch, we will keep reading the same file
        }
    }
}
