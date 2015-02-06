AisStore
===============================================================================
AisStore is an online high-available database capable of archiving multiple terabytes of AIS-data with an insert rate of at least 100.000 messages/minute. 
Besides the archiving mechanism is features an AIS-data export tool that is limited only by disk-read speed. 
It also provides simple analytical capabilities based on
time, geography and identity. Furthermore queries based on smaller time intervals,
limited geographical areas, or single ships can be performed in real-time (less than 1 second).


Build Instructions
-------------------------------------------------------------------------------
Build prerequisites: Java 8 + Maven 3

    git clone https://github.com/dma-ais/AisStore.git
    cd AisStore/
    mvn install

You can find the command line interface jar to AisStore in ais-store-cli/target/ais-store-cli-xxxx.jar

Source Code Organization
-------------------------------------------------------------------------------
The project is organized in the following modules

ais-store-common     Common classes for AisStore
ais-store-raw        Raw-exporter and job implementation that works on raw Cassandra files
ais-store-cli        Command line interface to AisStore

Command line interface
-------------------------------------------------------------------------------
The command line interface (CLI) requires

- Java 8 Runtime Environment (JRE 1.8)
- A running Cassandra cluster with known IP:port of at least one node.

AIS Store Archiver
-------------------------------------------------------------------------------

AIS Store Exporter
-------------------------------------------------------------------------------

Deployment at DMA
-------------------------------------------------------------------------------
AisStore is currently deployed at the Danish Maritime Authority.
Where it currently processes around 60.000 messages per minute.

Preparing a Cassandra test cluster
-------------------------------------------------------------------------------
A quick and easy way to prepare a Cassandra cluster for test and development
based on Docker, is to use [Al Tobey's cassandra-docker project](https://github.com/tobert/cassandra-docker).

### Installing Docker
Make sure that you have installed the most recent stable version of Docker
on a supported platform. The following examples are based on Docker 1.4.1
running on an Ubuntu 14.04 LTS Server based on these
[installation instructions](https://docs.docker.com/installation/ubuntulinux/#ubuntu-trusty-1404-lts-64-bit).

### Installing Cassandra
To pull the Cassandra docker image, do this:

    sudo docker pull tobert/cassandra:2.1.2

### Launching Cassandra
To prepare first launch, issue this commands:

    cd ~
    mkdir cassandra
    mkdir cassandra/node.1

Then launch Cassandra like this:

    sudo docker run -d -v ~/cassandra/node.1:/data tobert/cassandra:2.1.2

Now we have single-node docker cluster running; with all of its data and configuration
stored in ~/cassandra/node.1 on the host machine.

Performing

    sudo docker ps

gives us the container ID (a hex number) and the container name (a memorable string) of the Docker
container running our Cassandra node.

To verify that Cassandra is indeed live and running inside the container, we can use the standard
Cassandra 'nodetool' inside the container, like this:

    sudo docker exec -it loving_archimedes nodetool status

Which should yield something like this:

    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens  Owns (effective)  Host ID                               Rack
    UN  127.0.0.1  60.38 KB   256     100.0%            12b5b518-a2b1-4d35-9a93-433af643a1a5  rack1

The UN acronym indicates that this node is Up and in Normal state.

### Installing the AisStore schema into Cassandra
For AisStore to function, its [schema](https://github.com/dma-ais/AisStore/blob/master/aisdata-schema.cql)
must be imported into Cassandra.

First make the aisdata-schema.cql file visible from the Docker container runing our Cassandra node:

    cd ~/cassandra/node.1
    mkdir cql
    cd cql
    wget https://raw.githubusercontent.com/dma-ais/AisStore/master/aisdata-schema.cql

Then execute this file using cqlsh in the container:

    sudo docker exec -it loving_archimedes cqlsh
    Connected to Docker Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.2 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> source '/data/cqlsh/aisdata-schema.cql';

This may yield a warning about inability to drop a non existing keyspace. This can safely be ignored. If
everything else went well, you can inspect the new keyspace in Cassandra like this:

    cqlsh> use aisdata;
    cqlsh:aisdata> select * from packets_time;

     timeblock | timehash | aisdata
    -----------+----------+---------

    (0 rows)
    cqlsh:aisdata>

We are now ready to start pumping AIS data into Cassandra using AisStore.
