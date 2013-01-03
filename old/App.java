package dk.dma.cassandra_test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.utils.Hex;

import com.google.common.hash.Hashing;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import dk.dma.ais.message.AisPosition;

/**
 * Hello world!
 * 
 */
public class App {
    private final ConcurrentHashMap<Class<?>, List<Field>> fields = new ConcurrentHashMap<>();

    private static final Set<Class<?>> VALID_TYPES = new HashSet<>(Arrays.asList(int.class, long.class, String.class,
            AisPosition.class, double.class, float.class));

    public static void main(String[] args) throws Exception {
        App app = new App();
        app.run();
    }

    List<Field> getFieldds(Object o) {
        Class<?> c = o.getClass();
        List<Field> l = fields.get(c);
        if (l == null) {
            l = new ArrayList<>();
            while (c != Object.class) {
                for (Field field : c.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())
                            && VALID_TYPES.contains(field.getType())) {
                        field.setAccessible(true);
                        l.add(field);
                    }
                }
                c = c.getSuperclass();
            }
            fields.put(c, Collections.unmodifiableList(l));
        }
        return fields.get(c);
    }

    public void run() throws Exception {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("Test Cluster")
                .forKeyspace("DMA")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1)
                                .setSeeds("127.0.0.1:9160"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();

        ColumnFamily<byte[], String> aisEventCF = new ColumnFamily<>("ais_events6", // Column Family Name
                BytesArraySerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer

        byte[] event = new byte[100];
        for (int i = 0; i < 1000000; i++) {
            ThreadLocalRandom.current().nextBytes(event);

            MutationBatch m = keyspace.prepareMutationBatch();
            write(m, aisEventCF, "test", event);
            try {
                OperationResult<Void> result = m.execute();
            } catch (ConnectionException e) {}
            if (i % 1000 == 0) {
                System.out.println(i);
            }
        }

        for (ColumnFamilyDefinition d : keyspace.describeKeyspace().getColumnFamilyList()) {
            System.out.println(d.getName());
        }

    }

    void putColumn3(ColumnListMutation<String> clm, Field f, Object value) throws IllegalAccessException {
        Class<?> type = f.getType();
        if (value == null) {
            return;
        }
        if (type == int.class) {
            clm.putColumn(f.getName(), f.getInt(value));
        } else if (type == long.class) {
            clm.putColumn(f.getName(), f.getLong(value));
        } else if (type == double.class) {
            clm.putColumn(f.getName(), f.getFloat(value));
        } else if (type == float.class) {
            clm.putColumn(f.getName(), f.getDouble(value));
        } else if (type == String.class) {
            String v = (String) f.get(value);
            if (v != null && v.length() > 0) {
                clm.putColumn(f.getName(), v);
            }
        } else if (type == AisPosition.class) {
            AisPosition ap = (AisPosition) f.get(value);
            clm.putColumn(f.getName() + "Lat", ap.getRawLatitude());
            clm.putColumn(f.getName() + "Lon", ap.getRawLongitude());
        } else {
            throw new Error("Unknown datatype " + type);
        }
    }

    public void write(MutationBatch mb, ColumnFamily<byte[], String> cf, String source, byte[] event) throws Exception {
        byte[] key = Hashing.sha256().hashBytes(event).asBytes();

        ColumnListMutation<String> row = mb.withRow(cf, key);
        row.putColumn("insertDate", new Date(), null);
        row.putColumn("data", event, null);

        Object o = parseEvent(event);
        List<Field> fields = getFields(o);
        for (Field f : fields) {
            putColumn(row, f, o);
        }
    }

    private Object parseEvent(byte[] event) {
        return Hex.bytesToHex(event);
    }

    List<Field> getFields(Object o) {
        Class<?> c = o.getClass();
        List<Field> l = fields.get(c);
        if (l == null) {
            l = new ArrayList<>();
            while (c != Object.class) {
                for (Field field : c.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())
                            && VALID_TYPES.contains(field.getType())) {
                        field.setAccessible(true);
                        l.add(field);
                    }
                }
                c = c.getSuperclass();
            }
            fields.put(c, Collections.unmodifiableList(l));
        }
        return fields.get(c);
    }

    void putColumn(ColumnListMutation<String> clm, Field f, Object value) throws Exception {
        Class<?> type = f.getType();
        if (type == int.class) {
            clm.putColumn(f.getName(), f.getInt(value));
        } else if (type == long.class) {
            clm.putColumn(f.getName(), f.getLong(value));
        } else if (type == double.class) {
            clm.putColumn(f.getName(), f.getFloat(value));
        } else if (type == float.class) {
            clm.putColumn(f.getName(), f.getDouble(value));
        } else if (type == String.class) {
            clm.putColumn(f.getName(), (String) f.get(value));
        } else if (type == AisPosition.class) {
            AisPosition ap = (AisPosition) f.get(value);
            clm.putColumn(f.getName() + "Lat", ap.getRawLatitude());
            clm.putColumn(f.getName() + "Lon", ap.getRawLongitude());
        } else {
            throw new Error("Unknown datatype " + type);
        }
    }

}
