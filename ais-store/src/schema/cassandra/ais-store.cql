DROP TABLE aismessages;

CREATE TABLE aismessages (
  key blob PRIMARY KEY,
  message ascii,
  messagetype int,
  mmsi bigint,
  time bigint,
  timeday int,
  timehour int,
  timeminute int,
  cell001 bigint,
  cell01 bigint,
  cell1 bigint,
  ) WITH comment='Ais Message Data';

CREATE INDEX aismessages_messagetype
  ON aismessages (messagetype);
CREATE INDEX aismessages_mmsi
  ON aismessages (mmsi);
CREATE INDEX aismessages_timeday
  ON aismessages (timeday);
CREATE INDEX aismessages_timehour
  ON aismessages (timehour);
CREATE INDEX aismessages_timeminute
  ON aismessages (timeminute);
CREATE INDEX aismessages_cell001
  ON aismessages (cell001);
CREATE INDEX aismessages_cell01
  ON aismessages (cell01);
CREATE INDEX aismessages_cell1
  ON aismessages (cell1);


DROP TABLE aisships;

CREATE TABLE aisships (
  mmsi bigint PRIMARY KEY,
  time bigint,
  cell001 bigint,
  cell01 bigint,
  cell1 bigint,
  ) WITH comment='Ais Latest Ship Data';

CREATE INDEX aisships_cell001
  ON aisships (cell001);
CREATE INDEX aisships_cell01
  ON aisships (cell01);
CREATE INDEX aisships_cell1
  ON aisships (cell1);