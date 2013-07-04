drop column family messages_time;
drop column family messages_cell1;
drop column family messages_cell10;
drop column family messages_mmsi;
drop column family positions;


drop keyspace aisdata;



create keyspace aisdata with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and strategy_options = {replication_factor:1};

use aisdata; 
CREATE COLUMN FAMILY messages_time WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_cell1 WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_cell10 WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_mmsi WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:256};



CREATE TABLE sources (
    name text PRIMARY KEY,
    addresses set<text>
);




CREATE COLUMN FAMILY positions WITH key_validation_class= AsciiType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:128};


CREATE COLUMN FAMILY counters with default_validation_class=CounterColumnType;


DROP TABLE mmsi;

CREATE TABLE mmsi (
  mmsi int PRIMARY KEY,
  last_message ascii,
  last_message_timestamp bigint,
  last_position_message ascii,
  last_position_timestamp bigint,
  last_position_timehour int,
  last_position_timeminute int,
  last_position_timesecond int,
  last_position_cell001 bigint,
  last_position_cell01 bigint,
  last_position_cell1 bigint
  ) WITH comment='Latest mmsi Data';

CREATE INDEX mmsi_last_position_timehour
  ON mmsi (last_position_timehour);
CREATE INDEX mmsi_last_position_timeminute
  ON mmsi (last_position_timeminute);
CREATE INDEX mmsi_last_position_timesecond
  ON mmsi (last_position_timesecond);
CREATE INDEX mmsi_last_position_cell001
  ON mmsi (last_position_cell001);
CREATE INDEX mmsi_last_position_cell01
  ON mmsi (last_position_cell01);
CREATE INDEX mmsi_last_position_cell1
  ON mmsi (last_position_cell1);
  
  
  
  

CREATE TABLE positions (user ascii PRIMARY KEY) with compression={'sstable_compression':'DeflateCompressor', 'chunk_length_kb':128};
 
 
 
CREATE TABLE messages_time (user int PRIMARY KEY)
with compression_options={'sstable_compression':'DeflateCompressor', 'chunk_length_kb':1024};


WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
 
 
CREATE COLUMN FAMILY messages_time WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_cell10 WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_cell1 WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:256};
CREATE COLUMN FAMILY messages_mmsi WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:256};
CREATE COLUMN FAMILY positions WITH key_validation_class= AsciiType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:128};


CREATE COLUMN FAMILY counters with default_validation_class=CounterColumnType;


DROP TABLE mmsi;

CREATE INDEX mmsi_last_position_timehour
  ON mmsi (last_position_timehour);
CREATE INDEX mmsi_last_position_timeminute
  ON mmsi (last_position_timeminute);
CREATE INDEX mmsi_last_position_timesecond
  ON mmsi (last_position_timesecond);
CREATE INDEX mmsi_last_position_cell001
  ON mmsi (last_position_cell001);
CREATE INDEX mmsi_last_position_cell01
  ON mmsi (last_position_cell01);
CREATE INDEX mmsi_last_position_cell1
  ON mmsi (last_position_cell1);
  