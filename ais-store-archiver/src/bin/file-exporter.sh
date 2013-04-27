#!/bin/sh

CP=.
for i in `ls lib/*.jar`
do
  CP=${CP}:${i}
done


java -server -cp $CP -Xmn256M -Xms512M -Xmx1024M dk.dma.ais.store.export.AisStoreFileExporter $@
