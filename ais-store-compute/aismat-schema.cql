CREATE KEYSPACE aismat 
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
	   
USE aismat;

DROP TABLE mmsi_time_count;
CREATE TABLE mmsi_time_count (
    mmsi int,
    result counter,
	time text,
    PRIMARY KEY (mmsi, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';

DROP TABLE source_time_count;
CREATE TABLE source_time_count (
    sourceid text,
    result counter,
	time text,
    PRIMARY KEY (source, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';

DROP TABLE cell1_source_time_count;
CREATE TABLE cell1_source_time_count (
    cellid int,
    result counter,
	time text,
    PRIMARY KEY (cell1, source, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';

DROP TABLE cell1_time_count;
CREATE TABLE cell1_time_count (
    cellid int,
    result counter,
	time text,
    PRIMARY KEY (source, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';
	
DROP TABLE stream_monitor;
CREATE TABLE stream_monitor (
	time text,
	status text,
	count counter,
	PRIMARY KEY (time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';	