
CREATE KEYSPACE aismat 
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
	   
USE aismat;

DROP TABLE mmsi_time_count;
CREATE TABLE mmsi_time_count (
    mmsi int,
    result int,
	time int,
    PRIMARY KEY (mmsi, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';

DROP TABLE source_time_count;
CREATE TABLE source_time_count (
    sourceid text,
    result int,
	time int,
    PRIMARY KEY (sourceid, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';

DROP TABLE cell1_source_time_count;
CREATE TABLE cell1_source_time_count (
    cellid int,
    sourceid text,
    result int,
	time int,
    PRIMARY KEY (cellid, sourceid, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';

DROP TABLE cell1_time_count;
CREATE TABLE cell1_time_count (
    cellid int,
    result int,
	time int,
    PRIMARY KEY (cellid, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';
	
DROP TABLE stream_monitor;
CREATE TABLE stream_monitor (
	timeblock int,
	PRIMARY KEY (timeblock))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';	
