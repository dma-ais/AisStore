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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.commons.util.io.OutputStreamSink;

/**
 * 
 * @author Kasper Nielsen
 */
public class ReflectionBasedTableOutputStreamSink extends OutputStreamSink<AisPacket> {

    /** Just a placeholder for NULL in the methods map. */
    private static final Method NULL = ReflectionBasedTableOutputStreamSink.class.getDeclaredMethods()[0];

    private final String[] columns;

    private final AtomicLong counter = new AtomicLong();

    /** A cache of methods. */
    private final ConcurrentHashMap<Map.Entry<Class<?>, String>, Method> methods = new ConcurrentHashMap<>();

    /** A date formatter. */
    private final DateTimeFormatter fmt = ISODateTimeFormat.dateTime();

    ReflectionBasedTableOutputStreamSink(String format) {
        columns = format.split(";");
    }

    /** {@inheritDoc} */
    @Override
    public void process(OutputStream stream, AisPacket message) throws IOException {
        AisMessage m = message.tryGetAisMessage();
        if (m != null) {
            long n = counter.incrementAndGet();
            for (int i = 0; i < columns.length; i++) {
                String c = columns[i];
                if (c.equals("n")) {
                    stream.write(Long.toString(n).getBytes(StandardCharsets.US_ASCII));
                } else if (c.equals("timestamp")) {
                    stream.write(Long.toString(message.getBestTimestamp()).getBytes(StandardCharsets.US_ASCII));
                } else if (c.equals("time")) {
                    DateTime dateTime = new DateTime(new Date(message.getBestTimestamp()));
                    String str = fmt.print(dateTime);
                    stream.write(str.getBytes(StandardCharsets.US_ASCII));
                } else {
                    Method g = findGetter(c, m.getClass());
                    if (g != null) {
                        try {
                            Object o = g.invoke(m);
                            String s = o.toString();
                            stream.write(s.getBytes(StandardCharsets.US_ASCII));
                        } catch (InvocationTargetException | IllegalAccessException e) {
                            throw new IOException(e);
                        }
                    }
                }
                if (i < columns.length - 1) {
                    stream.write(';');
                }
            }
            stream.write('\n');
        }
    }

    /** {@inheritDoc} */
    @Override
    public void header(OutputStream stream) throws IOException {
        // Writes the name of each column as the header
        for (int i = 0; i < columns.length; i++) {
            stream.write(columns[i].getBytes(StandardCharsets.US_ASCII));
            if (i < columns.length - 1) {
                stream.write(';');
            }
        }
        stream.write('\n');
    }

    private Method findGetter(String nameOfColumn, Class<?> type) throws IOException {
        Entry<Class<?>, String> key = new SimpleImmutableEntry<Class<?>, String>(type, nameOfColumn);
        Method m = methods.get(key);
        if (m == null) {
            m = NULL;
            BeanInfo info = null;
            try {
                info = Introspector.getBeanInfo(type);
            } catch (IntrospectionException e) {
                throw new IOException(e);
            }
            for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
                if (nameOfColumn.equals(pd.getName())) {
                    m = pd.getReadMethod();
                }
            }
            methods.put(key, m);
        }
        return m == NULL ? null : m;
    }
}
