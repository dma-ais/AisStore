cd /AisStore

BACKUPP=""
if [ "${BACKUP}" ]; then 
	BACKUPP="-backup ${BACKUP}"
fi

BATCHSIZEP=""
if [ "${BATCHSIZE}" ]; then 
	BATCHSIZEP="-batchSize ${BATCHSIZE}"
fi

DATABASEKEYSPACE=""
if [ "${KEYSPACE}" ]; then 
	DATABASEKEYSPACE="-databaseName ${KEYSPACE}"
fi

#print help if command not given
COMMANDP="-help"
if [ "${COMMAND}" ]; then 
	COMMANDP=$COMMAND
fi

JAR=`ls ais-store-cli/target/ais-store-cli*.jar`
java -jar $JAR $COMMANDP $SOURCES $BACKUPP $BATCHSIZEP -database $DATABASE $DATABASEKEYSPACE