DROP TABLE aismessages;

CREATE TABLE aismessages (
  k blob PRIMARY KEY,
  m ascii,
  h bigint,
  ) WITH comment='Ais Message Data';

CREATE INDEX aismessages_mmsi_hour
  ON aismessages (h);

drop column family messages_time;
drop column family messages_zip;
drop column family messages_mmsi;
drop column family positions;

CREATE COLUMN FAMILY messages_time WITH key_validation_class= IntegerType and compression_options={sstable_compression:SnappyCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_zip WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:1024};
CREATE COLUMN FAMILY messages_mmsi WITH key_validation_class= IntegerType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:64};
CREATE COLUMN FAMILY positions WITH key_validation_class= AsciiType and compression_options={sstable_compression:DeflateCompressor, chunk_length_kb:128};

create column family messages_time with comparator = BytesType;   
create column family messages_mmsi with comparator = BytesType;
create column family positions with comparator = BytesType;   



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
  