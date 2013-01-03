DROP TABLE ais_baseline_zip_ascii_archive;

CREATE TABLE ais_baseline_zip_ascii_archive (
  m ascii PRIMARY KEY,
  s ascii
) WITH comment='Ais Message Data';
   compression_parameters:sstable_compression = 'DeflateCompressor'
  
   and compression_options = [{'sstable_compression' : 'DeflateCompressor'}];

   
   
   CREATE KEYSPACE ks WITH replication = { 'class' : 'SimpleStrategy',
                                                'replication_factor' : 2 };