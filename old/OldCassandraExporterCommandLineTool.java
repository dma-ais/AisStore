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
package dk.dma.ais.store.export;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.store.cassandra.CassandraSSTableProcessor;
import dk.dma.ais.store.cassandra.Tables;
import dk.dma.app.AbstractCommandLineTool;
import dk.dma.app.util.FormatUtil;

/**
 * 
 * @author Kasper Nielsen
 */
public class OldCassandraExporterCommandLineTool extends AbstractCommandLineTool {

    @Parameter(names = "-chunksize", description = "The maximum size of each outputfile in mb")
    int chunkSizeMB = 1000;

    @Parameter(names = "-keyspace", description = "The keyspace to use")
    private final String keyspace = "DMA";

    @Parameter(names = "-exporter", description = "The exporter class")
    private final String exporter = DefaultExportFunction.class.getCanonicalName();

    @Parameter(names = "-cassandra", description = "The full path to cassandra.yaml", required = true)
    private File cassandra;

    @Parameter(names = "-destination", description = "Destination folder for Output")
    private final File destination = new File(".");

    @Parameter(names = "-noZip", description = "Does not compress the exported data")
    private boolean noZip;

    private ExportFunction function;

    /**
     * @param appname
     */
    public OldCassandraExporterCommandLineTool() {
        super("CassandraExporter");
    }

    public static void main(String[] args) throws Exception {
        args = new String[] { "-cassandra", "/Applications/apache-cassandra-1.1.6/conf/cassandra.yaml", "-noZip" };
        // args = new String[] { "-help" };

        new OldCassandraExporterCommandLineTool().execute(args);
    }

    /** {@inheritDoc} */
    @Override
    protected void execute(String[] args) throws Exception {
        JCommander jc = new JCommander(this, args);

        @SuppressWarnings("unchecked")
        Class<ExportFunction> exportClass = (Class<ExportFunction>) Class.forName(exporter);
        function = exportClass.newInstance();
        jc = new JCommander();
        jc.addObject(function);
        jc.addObject(this);

        jc.setProgramName(getApplicationName());
        if (help) {
            help(jc);
            System.exit(0);
        }
        start();
    }

    /** {@inheritDoc} */
    @Override
    protected void configure() {}

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     */
    @Override
    protected void run(Injector injector) throws Exception {
        validateFolder("Destination", destination);

        CassandraSSTableProcessor csp = new CassandraSSTableProcessor(cassandra.toString(), keyspace);
        // Check if there is cassandra files
        // DatabaseDescriptor needs this system property
        System.setProperty("cassandra.config", "file://" + cassandra);

        DatabaseDescriptor.loadSchemas();
        if (Schema.instance.getNonSystemTables().size() < 1) {
            throw new ConfigurationException("no non-system tables are defined");
        }
        System.out.println("Exporting to " + destination.getAbsolutePath());

        // Take a snapshot that we will work on
        String snapshotName = new Date().toString().replace(' ', '_').replace(':', '_');
        CassandraSnapshotWriter.takeSnapshot(keyspace, snapshotName);

        long start = System.nanoTime();
        try {
            RollingOutputStream ros = new RollingOutputStream(destination.toPath(), "aismessages", chunkSizeMB, !noZip);
            try {
                for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
                    Path snapshots = Paths.get(s).resolve(keyspace).resolve(Tables.AIS_MESSAGES).resolve("snapshots")
                            .resolve(snapshotName);
                    // iterable through all data files (xxxx-Data)
                    // if the dataformat changes hf needs to be upgraded to the current versino
                    // http://svn.apache.org/repos/asf/cassandra/trunk/src/java/org/apache/cassandra/io/sstable/Descriptor.java
                    try (DirectoryStream<Path> ds = Files.newDirectoryStream(snapshots, keyspace + "-"
                            + Tables.AIS_MESSAGES + "-hf-*-Data.db")) {
                        for (Path p : ds) { // for each data file
                            System.out.println("Exporting from " + p);
                            exportFile(p.toString(), function, ros);// export file
                        }
                    }
                }
            } finally {
                ros.closeIfOpen();
                start = System.nanoTime() - start;
                long s = TimeUnit.NANOSECONDS.toSeconds(start);
                double minuteAvg = ros.totalWritten * ((double) TimeUnit.MINUTES.toNanos(1) / (double) start);

                System.out.println("Wrote a total of " + FormatUtil.humanReadableByteCount(ros.totalWritten, true)
                        + " in " + String.format("%d:%02d:%02d", s / 3600, s % 3600 / 60, s % 60) + " ("
                        + FormatUtil.humanReadableByteCount((long) minuteAvg, true) + "/min)");
            }
        } finally {
            CassandraSnapshotWriter.deleteSnapshot(keyspace, snapshotName);
        }
    }

    private void exportFile(String ssTableFile, ExportFunction exportFunction, RollingOutputStream os)
            throws IOException {
        SSTableReader reader = SSTableReader.open(Descriptor.fromFilename(ssTableFile));
        SSTableScanner scanner = reader.getDirectScanner();
        System.out.println("exporting " + ssTableFile);
        System.out.println("estimated keys" + reader.estimatedKeys());
        try {
            byte[] messageColumn = Tables.AIS_MESSAGES_MESSAGE.getBytes();
            while (scanner.hasNext()) {
                IColumnIterator columnIterator = scanner.next();
                try {
                    while (columnIterator.hasNext()) {
                        IColumn c = columnIterator.next();
                        if (Arrays.equals(messageColumn, c.name().array())) {
                            byte[] value = c.value().array();
                            String msg = new String(value);
                            exportFunction.export(msg, 123L, os);
                            os.checkWrap();
                        }
                    }
                } finally {
                    columnIterator.close();
                }
            }
        } finally {
            scanner.close();
        }
    }
}
