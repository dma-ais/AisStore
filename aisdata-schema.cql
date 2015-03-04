DROP KEYSPACE aisdata;

CREATE KEYSPACE aisdata_test WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': '2' };

USE aisdata_test;

CREATE TABLE packets_time (
    timeblock int,
    time timestamp,
    digest blob,
    aisdata ascii,
    PRIMARY KEY (timeblock, time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by receive time.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'tombstone_compaction_interval':'2592000'}
    AND caching = 'keys_only';

CREATE TABLE packets_mmsi (
    mmsi int,
    time timestamp,
    digest blob,
    aisdata ascii,
    PRIMARY KEY (mmsi, time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by mmsi number.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'tombstone_compaction_interval':'2592000'}
    AND caching = 'keys_only';

CREATE TABLE packets_area_cell1 (
    cellid int,
    timeblock int,
    time timestamp,
    digest blob,
    aisdata ascii,
    PRIMARY KEY ((cellid, timeblock), time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by cells of size 1 degree.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'tombstone_compaction_interval':'2592000'}
    AND caching = 'keys_only';

CREATE TABLE packets_area_cell10 (
    cellid int,
    timeblock int,
    time timestamp,
    digest blob,
    aisdata ascii,
    PRIMARY KEY ((cellid, timeblock), time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024}
    AND comment = 'AIS data ordered by cells of size 10 degree.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'tombstone_compaction_interval':'2592000'}
    AND caching = 'keys_only';


CREATE TABLE packets_area_unknown (
    mmsi int,
    time timestamp,
    digest blob,
    aisdata ascii,
    PRIMARY KEY (mmsi, time, digest)
)
WITH
    compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':64}
    AND comment = 'AIS data where the area has not yet been determined.'
    AND speculative_retry = '99percentile'
    AND compaction = {'class': 'SizeTieredCompactionStrategy', 'tombstone_compaction_interval':'2592000'}
    AND caching = 'keys_only';
