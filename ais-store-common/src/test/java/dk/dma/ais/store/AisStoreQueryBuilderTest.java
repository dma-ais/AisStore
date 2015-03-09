package dk.dma.ais.store;

import dk.dma.ais.packet.AisPacket;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.model.geometry.BoundingBox;
import dk.dma.enav.model.geometry.CoordinateSystem;
import dk.dma.enav.model.geometry.Position;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.Iterator;

@Ignore
public class AisStoreQueryBuilderTest {

    static CassandraConnection connection;

    @BeforeClass
    public static void setup() {
        connection = CassandraConnection.create("aisdata", "192.168.1.101");
        connection.startAsync();
    }

    @Test
    public void testForTime() throws Exception {
        AisStoreQueryBuilder queryBuilder = AisStoreQueryBuilder
                .forTime()
                .setInterval(Instant.parse("2015-03-06T05:00:00Z"), Instant.parse("2015-03-06T06:00:00Z"));

        AisStoreQueryResult queryResult = queryBuilder.execute(connection.getSession());

        Iterator<AisPacket> iterator = queryResult.iterator();
        while(iterator.hasNext()) {
            System.out.println(iterator.next().getStringMessage());
        }
    }

    @Test
    public void testForMmsi() throws Exception {
        AisStoreQueryBuilder queryBuilder = AisStoreQueryBuilder
                .forMmsi(357987000)
                .setInterval(Instant.parse("2015-03-01T13:46:06Z"), Instant.parse("2015-03-15T23:59:59Z"));

        AisStoreQueryResult queryResult = queryBuilder.execute(connection.getSession());

        Iterator<AisPacket> iterator = queryResult.iterator();
        while(iterator.hasNext()) {
            System.out.println(iterator.next().getStringMessage());
        }
    }

    @Test
    public void testForMmsis() throws Exception {
        AisStoreQueryBuilder queryBuilder = AisStoreQueryBuilder
                .forMmsi(357987000, 725000317)
                .setInterval(Instant.parse("2015-03-01T13:46:06Z"), Instant.parse("2015-03-15T23:59:59Z"));

        AisStoreQueryResult queryResult = queryBuilder.execute(connection.getSession());

        Iterator<AisPacket> iterator = queryResult.iterator();
        while(iterator.hasNext()) {
            System.out.println(iterator.next().getStringMessage());
        }
    }

    @Test
    public void testForArea1() throws Exception {
        AisStoreQueryBuilder queryBuilder = AisStoreQueryBuilder
                .forArea(BoundingBox.create(Position.create(55.8487945,10.0156428), Position.create(55.817993,10.1317718), CoordinateSystem.CARTESIAN))
                .setInterval(Instant.parse("2015-03-01T00:00:00Z"), Instant.parse("2015-03-15T23:59:59Z"));

        AisStoreQueryResult queryResult = queryBuilder.execute(connection.getSession());

        Iterator<AisPacket> iterator = queryResult.iterator();
        while(iterator.hasNext()) {
            System.out.println(iterator.next().getStringMessage());
        }
    }

    @Test
    public void testForArea10() throws Exception {
        AisStoreQueryBuilder queryBuilder = AisStoreQueryBuilder
                .forArea(BoundingBox.create(Position.create(60, -10), Position.create(40,20), CoordinateSystem.CARTESIAN))
                .setInterval(Instant.parse("2015-03-01T13:46:06Z"), Instant.parse("2015-03-15T23:59:59Z"));

        AisStoreQueryResult queryResult = queryBuilder.execute(connection.getSession());

        Iterator<AisPacket> iterator = queryResult.iterator();
        while(iterator.hasNext()) {
            System.out.println(iterator.next().getStringMessage());
        }
    }

}