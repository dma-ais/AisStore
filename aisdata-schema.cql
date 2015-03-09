DROP KEYSPACE aisdata;

CREATE KEYSPACE aisdata WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': '2' };

USE aisdata;

CREATE TABLE packets_time (
    timeblock int,   -- 10 minute time block since Epoch
    time timestamp,  -- Message receive time
    digest blob,     -- Murmur3 digest of aisdata
    aisdata ascii,   -- Raw AisPacket
    PRIMARY KEY (timeblock, time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by receive time.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'3600', 'min_threshold':'4', 'max_sstable_age_days':'30'}
    AND caching = 'keys_only';

CREATE TABLE packets_mmsi (
    mmsi int,        -- MMSI no.
    timeblock int,   -- 30 day time block since Epoch (~10 MB per partition)
    time timestamp,  -- Message receive time
    digest blob,     -- Murmur3 digest of aisdata
    aisdata ascii,   -- Raw AisPacket
    PRIMARY KEY ((mmsi, timeblock), time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by mmsi number.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'3600', 'min_threshold':'4', 'max_sstable_age_days':'30'}
    AND caching = 'keys_only';

CREATE TABLE packets_area_cell1 (
    cellid int,      -- Geographical cell id from DMA Grid
    timeblock int,   -- 10 minute time period since Epoch
    time timestamp,  -- Message receive time
    digest blob,     -- Murmur3 digest of aisdata
    aisdata ascii,   -- Raw AisPacket
    PRIMARY KEY ((cellid, timeblock), time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by cells of size 1 degree.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'3600', 'min_threshold':'4', 'max_sstable_age_days':'30'}
    AND caching = 'keys_only';

CREATE TABLE packets_area_cell10 (
    cellid int,      -- Geographical cell id from DMA Grid
    timeblock int,   -- 10 minute time period since Epoch
    time timestamp,  -- Message receive time
    digest blob,     -- Murmur3 digest of aisdata
    aisdata ascii,   -- Raw AisPacket
    PRIMARY KEY ((cellid, timeblock), time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by cells of size 10 degree.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'3600', 'min_threshold':'4', 'max_sstable_age_days':'30'}
    AND caching = 'keys_only';

CREATE TABLE packets_area_unknown (
    mmsi int,        -- MMSI no.
    timeblock int,   -- 30 day time block since Epoch (~10 MB per partition)
    time timestamp,  -- Message receive time
    digest blob,     -- Murmur3 digest of aisdata
    aisdata ascii,   -- Raw AisPacket
    PRIMARY KEY ((mmsi, timeblock), time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':64}
    AND comment = 'AIS data where the area has not yet been determined.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'3600', 'min_threshold':'4', 'max_sstable_age_days':'30'}
    AND caching = 'keys_only';
