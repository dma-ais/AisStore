DROP TABLE messages;

CREATE TABLE messages (
  key blob PRIMARY KEY,
  message ascii,
  mmsi_hour bigint,
  cell001_minute bigint,
  cell01_minute bigint,
  cell1_minute bigint,
  cell001_hour bigint,
  cell01_hour bigint,
  cell1_hour bigint
  ) WITH comment='Ais Message Data';

CREATE INDEX messages_mmsi_hour
  ON messages (mmsi_hour);

CREATE INDEX messages_cell001_minute
  ON messages (cell001_minute);
CREATE INDEX messages_cell01_minute
  ON messages (cell01_minute);
CREATE INDEX messages_cell1_minute
  ON messages (cell1_minute);

CREATE INDEX messages_cell001_hour
  ON messages (cell001_hour);
CREATE INDEX messages_cell01_hour
  ON messages (cell01_hour);
CREATE INDEX messages_cell1_hour
  ON messages (cell1_hour);


DROP TABLE mmsi;

CREATE TABLE mmsi (
  mmsi bigint PRIMARY KEY,
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
  