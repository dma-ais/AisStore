
if [ -z "${AIS_VIEW_URL}" ]; then
	AIS_VIEW_URLP=""
else
	AIS_VIEW_URLP="--ais.view.url=${AIS_VIEW_URL}"
fi

if [ -z "${REPO_PATH}" ]; then
	REPO_PATHP=""
else
	REPO_PATHP="--repo.root=${REPO_PATH}"
fi

if [ -z "${AUTH_HEADER}" ]; then
	AUTH_HEADERP=""
else
	AUTH_HEADERP="--auth.header=${AUTH_HEADER}"
fi

LATEST=`ls /archive/target/ais-downloader*SNAPSHOT.war`

java -jar $LATEST $AIS_VIEW_URLP $REPO_PATHP AUTH_HEADERP
