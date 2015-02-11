DROP KEYSPACE aisdata;

CREATE KEYSPACE aisdata WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': '2' };
USE aisdata;

CREATE TABLE packets_time (
    timeblock int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (timeblock, timehash)
    ) 
  WITH compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':2048} AND
       comment     = 'Contains AIS data ordered by receive time.'
       AND speculative_retry = '99percentile'
       AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'600', 'max_sstable_age_days':'365'};
       AND caching = 'keys_only';

CREATE TABLE packets_mmsi (
    mmsi int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (mmsi, timehash)
    ) 
  WITH compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':256} AND
       comment     = 'Contains AIS data ordered by mmsi number.'
       AND speculative_retry = '99percentile'
       AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'600', 'max_sstable_age_days':'365'};
       AND caching = 'keys_only';

CREATE TABLE packets_area_cell1 (
    cellid int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (cellid, timehash)
    ) 
  WITH compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024} AND
       comment     = 'Contains AIS data ordered by cells of size 1 degree.'
       AND speculative_retry = '99percentile'
       AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'600', 'max_sstable_age_days':'365'};
       AND caching = 'keys_only';

CREATE TABLE packets_area_cell10 (
    cellid int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (cellid, timehash)
    ) 
  WITH compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':1024} AND
       comment     = 'Contains AIS data ordered by cells of size 10 degree.'
       AND speculative_retry = '99percentile'
       AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'600', 'max_sstable_age_days':'365'};
       AND caching = 'keys_only';


CREATE TABLE packets_area_unknown (
    mmsi int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (mmsi, timehash)
    ) 
  WITH compression = {'sstable_compression':'LZ4Compressor', 'chunk_length_kb':256} AND
       comment     = 'Contains AIS data where the area has not yet been determined.' 
       AND speculative_retry = '99percentile'
       AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'MICROSECONDS', 'base_time_seconds':'600', 'max_sstable_age_days':'365'};
       AND caching = 'none';

