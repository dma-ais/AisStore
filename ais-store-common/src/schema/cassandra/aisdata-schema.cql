DROP KEYSPACE aisdata;

CREATE KEYSPACE aisdata 
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE aisdata;

CREATE TABLE packets_time (
    timeblock int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (timeblock, timehash)
    ) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND
       comment     = 'Contains AIS data ordered by receive time.';

CREATE TABLE packets_mmsi (
    mmsi int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (mmsi, timehash)
    ) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':256} AND
       comment     = 'Contains AIS data ordered by mmsi number.';

CREATE TABLE packets_area_cell1 (
    cellid int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (cellid, timehash)
    ) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND
       comment     = 'Contains AIS data ordered by cells of size 1 degree.';

CREATE TABLE packets_area_cell10 (
    cellid int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (cellid, timehash)
    ) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND
       comment     = 'Contains AIS data ordered by cells of size 10 degree.';


CREATE TABLE packets_area_unknown (
    mmsi int,
    timehash blob,
    aisdata blob,
    PRIMARY KEY (mmsi, timehash)
    ) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':256} AND
       comment     = 'Contains AIS data where the area has not yet been determined.';

