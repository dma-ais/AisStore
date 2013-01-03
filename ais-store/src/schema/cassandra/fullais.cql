DROP TABLE messages;

CREATE TABLE messages (
  key blob PRIMARY KEY,
  message ascii,
  messagetype int,
  mmsi bigint,
  timeday int,
  timehour int,
  timeminute int,
  cell001 bigint,
  cell01 bigint,
  cell1 bigint,
  ) WITH comment='Ais Message Data';

CREATE INDEX messages_messagetype
  ON messages (messagetype);
CREATE INDEX messages_mmsi
  ON messages (mmsi);
CREATE INDEX messages_timeday
  ON messages (timeday);
CREATE INDEX messages_timehour
  ON messages (timehour);
CREATE INDEX messages_timeminute
  ON messages (timeminute);
CREATE INDEX messages_cell001
  ON messages (cell001);
CREATE INDEX messages_cell01
  ON messages (cell01);
CREATE INDEX messages_cell1
  ON messages (cell1);

DROP TABLE last_position;

CREATE TABLE last_position (
  mmsi bigint PRIMARY KEY,
  message ascii,
  time bigint,
  timeday int,
  timehour int,
  timeminute int,
  timesecond int,
  cell001 bigint,
  cell01 bigint,
  cell1 bigint,
  ) WITH comment='Ais Latest Ship Data';

CREATE INDEX last_position_cell001
  ON last_position (cell001);
CREATE INDEX last_position_cell01
  ON last_position (cell01);
CREATE INDEX last_position_cell1
  ON last_position (cell1);
CREATE INDEX last_position_timeday
  ON last_position (timeday);
CREATE INDEX last_positiontimehour
  ON last_position (timehour);
CREATE INDEX last_position_timeminute
  ON last_position (timeminute);
CREATE INDEX last_position_timesecond
  ON last_position (timesecond);
  