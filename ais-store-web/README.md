AIS-Downloader
==========================

A tool for querying the AIS Store database.

## Requirements

On the client side we use:

* JavaScript/HTML
* AngularJS (for forms and calling webservices)
* Twitter Bootstrap (for basic layout)
* JQuery (limited use for some DOM-manipulation)
* HTML5 Application Cache
* OpenLayers

On the server side we use:

* Java 8
* Maven (for building)
* SpringBoot (for packaging single jar service and REST)


## Building ##

    mvn clean install

## Launch

The build produces a executable .war-file in the /target folder. The application can be launched with:

    java -jar target/ais-downloader-0.1-SNAPSHOT.war

or by using maven:

    mvn spring-boot:run

A local deployment will setup AIS-Downloader at the following URL:

    http://localhost:8080/downloader/query.html

## Docker

The AIS-Downloader is being built at docker.io [https://registry.hub.docker.com/u/dmadk/ais-downloader/](https://registry.hub.docker.com/u/dmadk/ais-downloader/)

The base command for running dmadk/ais-downloader is:

    sudo docker run dmadk/ais-downloader

For a more advanced example, if you want run dmadk/ais-downloader such that it binds to localhost:8081, preserves the downloaded files under /tmp/aisdownloader on the host,
and adds a hello:mum basic authentication header to the AIS Store URL, use:

    sudo docker run -d -p 8081:8080 \
         -e REPO_PATH=/var/aisdownloader \
         -e AUTH_HEADER="Basic aGVsbG86bXVt"
         -v /tmp/aisdownloader:/var/aisdownloader dmadk/ais-downloader

