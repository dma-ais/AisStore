CREATE KEYSPACE aismat 
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
	   
USE aismat;

DROP TABLE mmsi_time_count;
CREATE TABLE mmsi_time_count (
    mmsi int,
    result bigint,
	time text,
	visited int,
    PRIMARY KEY (mmsi, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';


DROP TABLE source_time_count;
CREATE TABLE source_time_count (
    sourceid text,
    result counter,
	time text,
	visited int,
    PRIMARY KEY (source, time))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Materialized View Test schema';
		