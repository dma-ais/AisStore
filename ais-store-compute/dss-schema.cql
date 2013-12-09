CREATE KEYSPACE dss 
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
	   
USE dss;

CREATE TABLE triggered_time (
	timeblock int,
	timehash int,
	updatecheck long,
	PRIMARY KEY (timeblock)
    );
	
CREATE TABLE triggered_mmsi (
	mmsi int,
	updatecheck int,
	PRIMARY KEY (mmsi)
    );

	
CREATE TABLE dss_time (
	timeblock int,
	level string,
	result blob,
	updatecheck int,
	PRIMARY KEY (timeblock, level)
	);

DROP TABLE dss_mmsi_count;
CREATE TABLE dss_mmsi_count (
    mmsi int,
    result bigint,
	level text,
    scan bigint,
    PRIMARY KEY (mmsi, level, check))
	WITH compression = {'sstable_compression':'SnappyCompressor', 'chunk_length_kb':64}
	AND comment = 'Test of a materialized view of number of ais messages per mmsi number for given day';
	
	
DROP TRIGGER trigger_packets_time ON aisdata.packets_time;
CREATE TRIGGER trigger_packets_time 
    ON aisdata.packets_time 
    USING 'dk.dma.ais.store.compute.triggers.TimeBlockTrigger';
    
DROP TRIGGER trigger_packets_mmsi ON aisdata.packets_mmsi;
CREATE TRIGGER trigger_packets_mmsi
    ON aisdata.packets_mmsi
    USING 'dk.dma.ais.store.compute.triggers.MMSITrigger';    