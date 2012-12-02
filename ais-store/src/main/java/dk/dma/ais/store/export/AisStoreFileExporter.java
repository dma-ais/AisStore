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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.inject.Injector;

import dk.dma.ais.store.cassandra.CassandraSSTableProcessor;
import dk.dma.ais.store.query.ReceivedMessage;
import dk.dma.ais.store.util.QueuePumper;
import dk.dma.app.AbstractCommandLineTool;
import dk.dma.app.util.IoUtil;
import dk.dma.app.util.RollingOutputStream;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisStoreFileExporter extends AbstractCommandLineTool {

    @Parameter(names = "-cassandra", description = "The full path to cassandra.yaml", required = true)
    File cassandra;

    @Parameter(names = "-chunksize", description = "The maximum size of each outputfile in mb")
    int chunkSizeMB = 1000;

    @Parameter(names = "-destination", description = "Destination folder for Output")
    File destination = new File(".");

    @Parameter(names = "-exporter", description = "The exporter class")
    String exporter = DefaultExportFunction.class.getCanonicalName();

    @ParametersDelegate
    ExportFunction exporterInstance;

    @Parameter(names = "-keyspace", description = "The keyspace to use")
    String keyspace = "DMA";

    @Parameter(names = "-noZip", description = "Does not compress the exported data")
    boolean noZip;

    @Override
    protected void run(Injector injector) throws Exception {
        IoUtil.validateFolderExist("Destination", destination);
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());

        CassandraSSTableProcessor csp = new CassandraSSTableProcessor(cassandra.toString(), keyspace);

        try (RollingOutputStream ros = new RollingOutputStream(destination.toPath(), "aismessages", chunkSizeMB, !noZip)) {
            QueuePumper qp = new QueuePumper(new ReceivedMessage.Processor() {
                @Override
                public void process(ReceivedMessage message) throws Exception {
                    exporterInstance.export(message, IoUtil.notCloseable(ros));
                    ros.checkRoll();
                }
            }, 10000);

            // We use a separate single thread to write messages, this is around 70 % faster when writing zip files.
            // Than just using a single thread
            final ExecutorService es = Executors.newSingleThreadExecutor();
            es.submit(qp);
            csp.process(qp);
            es.shutdown();
            es.awaitTermination(1, TimeUnit.HOURS);
        }
    }

    public static void main(String[] args) throws Exception {
        args = new String[] { "-cassandra", "/Applications/apache-cassandra-1.1.6/conf/cassandra.yaml", "-noZip",
                "-start", "2012-11-12" };
        new AisStoreFileExporter().execute(args);
    }
}
