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
package dk.dma.ais.web.rest;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.json.JsonHierarchicalStreamDriver;
import com.thoughtworks.xstream.io.xml.XppDriver;

import dk.dma.commons.util.io.OutputStreamSink;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class XStreamOutputStreamSink<T> extends OutputStreamSink<T> {

    private final String name;

    private ObjectOutputStream out;

    private final OutputType outputType;

    private final XStream xstream;

    final Class<T> type;

    XStreamOutputStreamSink(Class<T> type, String name, String elementName, OutputType outputType) {
        this.type = requireNonNull(type);
        this.name = requireNonNull(name);
        this.outputType = requireNonNull(outputType);
        xstream = new XStream(outputType == OutputType.JSON ? new JsonHierarchicalStreamDriver() : new XppDriver());
        xstream.alias(elementName, type);
        xstream.registerConverter(new AisPacketConverter());
    }

    /** {@inheritDoc} */
    @Override
    public void footer(OutputStream stream) throws IOException {
        if (out != null) {
            out.close();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void header(OutputStream stream) throws IOException {
        if (outputType == OutputType.XML) {
            stream.write("<?xml version=\"1.0\"?>".getBytes(StandardCharsets.US_ASCII));
            stream.write('\n');
        }
        // We create the output stream here, because we do not have access to stream before
        out = xstream.createObjectOutputStream(new PrintWriter(stream), name);
    }

    boolean isPacketWriteable(T packet) {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void process(OutputStream stream, T message) throws IOException {
        if (isPacketWriteable(message)) {
            out.writeObject(message);
        }
    }

    abstract void write(T packet, HierarchicalStreamWriter writer, MarshallingContext context);

    static void w(HierarchicalStreamWriter writer, String name, Object o) {
        writer.startNode(name);
        writer.setValue(Objects.toString(o));
        writer.endNode();
    }

    class AisPacketConverter implements Converter {

        /** {@inheritDoc} */
        @SuppressWarnings("rawtypes")
        public boolean canConvert(Class cl) {
            return cl == type;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext context) {
            write((T) o, writer, context);
        }

        /** {@inheritDoc} */
        public Object unmarshal(HierarchicalStreamReader arg0, UnmarshallingContext arg1) {
            throw new UnsupportedOperationException();// for now
        }
    }

    public static enum OutputType {
        XML, JSON;
    }
}
