DROP TABLE aismessages;

CREATE TABLE ais_baseline_archive (
  k blob PRIMARY KEY,
  m ascii,
) WITH comment='Ais Message Data';
