#CQL3
DROP KEYSPACE aisdata;

CREATE KEYSPACE aisdata 
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE aisdata;

CREATE TABLE messages_time (timeblock int PRIMARY KEY) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024} AND
       comment     = 'Contains AIS data ordered by receive time. Each row containing 10 minutes of data';
       
CREATE TABLE messages_cell1 (cellid int PRIMARY KEY) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':256} AND
       comment     = 'Contains AIS data ordered by the cell id';
      
CREATE TABLE messages_mmsi (mmsi int PRIMARY KEY) 
  WITH compression = {'sstable_compression':'DeflateCompressor', 'chunk_length_kb':256} AND
       comment     = 'Contains AIS data ordered by mmsi number';
 
CREATE TABLE mmsi (
  mmsi int PRIMARY KEY,
  last_message ascii,
  last_message_timestamp bigint,
  last_position_message ascii,
  last_position_timestamp bigint,
  last_position_timehour int,
  last_position_timeminute int,
  last_position_timesecond int
  ) WITH comment='Latest mmsi Data';

