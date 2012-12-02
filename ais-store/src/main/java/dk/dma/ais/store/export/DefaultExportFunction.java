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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

import com.beust.jcommander.Parameter;

import dk.dma.ais.store.ReceivedMessage;

/**
 * 
 * @author Kasper Nielsen
 */
public class DefaultExportFunction implements ExportFunction {

    @Parameter(names = "-start", description = "[Exporter] Start date (inclusive), format == yyyy-MM-dd")
    private Date start;

    @Parameter(names = "-stop", description = "[Exporter] Stop date (exclusive), format == yyyy-MM-dd")
    private Date stop;

    String countries;

    // specify source
    // specify country

    // host

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    @Override
    public void export(ReceivedMessage msg, OutputStream os) throws IOException {
        os.write(msg.getStringMessage().getBytes());
        os.write('\n');
    }
}
