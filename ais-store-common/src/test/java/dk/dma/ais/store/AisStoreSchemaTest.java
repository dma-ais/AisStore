package dk.dma.ais.store;

import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;

public class AisStoreSchemaTest {

    @Test
    public void testTimeBlock() throws Exception {
        assertEquals(0, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.EPOCH));
        assertEquals(0, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.EPOCH.plus(9, ChronoUnit.MINUTES)));
        assertEquals(1, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.EPOCH.plus(10, ChronoUnit.MINUTES)));
        assertEquals(1856448, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T00:00:00Z")));
        assertEquals(1856448, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T00:05:00Z")));
        assertEquals(1856449, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T00:10:00Z")));
        assertEquals(1856450, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T00:20:00Z")));
        assertEquals(1856506, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T09:49:38Z")));
        assertEquals(1856523, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T12:30:00Z")));
        assertEquals(1856523, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T12:39:59Z")));
        assertEquals(1856591, AisStoreSchema.timeBlock(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T23:59:59Z")));
    }

    @Test
    public void testTimeBlocks() throws Exception {
        Integer[] timeBlocks = AisStoreSchema.timeBlocks(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.EPOCH, Instant.EPOCH);
        assertEquals(1, timeBlocks.length);
        assertEquals(0, timeBlocks[0].intValue());

        timeBlocks = AisStoreSchema.timeBlocks(AisStoreSchema.Table.TABLE_PACKETS_TIME, Instant.parse("2005-04-19T00:00:00Z"), Instant.parse("2005-04-19T23:59:59Z"));
        assertEquals(144, timeBlocks.length);
        for (int i=0; i<144; i++)
            assertEquals(1856448+i, timeBlocks[i].intValue());
    }
}
