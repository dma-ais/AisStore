#!/usr/bin/env bash
JAR=`ls /ais-store-rest-*.jar`
cp -p /data/application*.properties /
echo "Running: "
echo "java -jar $JAR -cp /data:."
java -jar $JAR -cp /data:.
