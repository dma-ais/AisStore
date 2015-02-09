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

Preparing a single-node Cassandra test cluster
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

### Configuring Cassandra
After the first launch of Cassandra performed above, the main Cassandra configuration file is now
located in ~/cassandra/node.1/conf - a file called cassandra.yaml.

For the current purposes we want to do absolutely minimal configuration of Cassandra. But, a single
configuration parameter needs to be set to that Cassandra will listen to TCP connections coming from
outside of the Docker container.

So - open ~/cassandra/node.1/conf/cassandra.yaml for editing - and ensure that listen_address is commented out,
and that listen_interface is set to eth0. Like this:

    # listen_address: 127.0.0.1
    listen_interface: eth0

Restart Cassandra to make the settings take effect:

    $ sudo docker kill loving_archimedes
    $ sudo docker run -d -v ~/cassandra/node.1:/data tobert/cassandra:2.1.2
    ...

Then ensure that the Address field reported by nodetool has changed into the private IP-range between the
Docker container and the host, like this:

    $ sudo docker ps
    ... (take note of container name)

    $ sudo docker exec -it backstabbing_feynman nodetool status

    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address      Load       Tokens  Owns (effective)  Host ID                               Rack
    UN  172.17.0.10  60.38 KB   256     100.0%            12b5b518-a2b1-4d35-9a93-433af643a1a5  rack1

I.e. Address field changed from 127.0.0.1 into 172.x.x.x.

### Installing the AisStore schema into Cassandra
For AisStore to function, its [schema](https://github.com/dma-ais/AisStore/blob/master/aisdata-schema.cql)
must be imported into Cassandra.

First make the aisdata-schema.cql file visible from the Docker container runing our Cassandra node:

    $ cd ~/cassandra/node.1
    $ mkdir cql
    $ cd cql
    $ wget https://raw.githubusercontent.com/dma-ais/AisStore/master/aisdata-schema.cql

Then execute this file using cqlsh in the container:

    $ sudo docker exec -it loving_archimedes cqlsh
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

### Mapping Cassandra ports to host machine
Just one more thing is needed before we can start pumping AIS data into Cassandra using AisStore: The
Cassandra ports mapped inside the Docker container must be mapped to local ports on the host machine,
so that applications outside the Docker container can reach it.

First kill the Docker container we have been using for the initial setup and schema import:

    $ sudo docker ps
    ...
    $ sudo docker kill loving_archimedes

Then re-launch a similar container, this time with the -P command line option:

    $ sudo docker run -P -d -v ~/cassandra/node.1:/data tobert/cassandra:2.1.2
    ...
    $ sudo docker ps

Now we can see from the ps-output, how the Cassandra ports are mapped to the host machine, e.g.:

    0.0.0.0:49159->22/tcp, 0.0.0.0:49160->61621/tcp, 0.0.0.0:49161->7000/tcp, 0.0.0.0:49162->7199/tcp, 0.0.0.0:49163->9042/tcp, 0.0.0.0:49164->9160/tcp

Importing file-based AIS data into a Cassandra cluster
------------------------------------------------------

### Prerequisites

- A directory with compressed (zipped) AIS data in NMEA format; possibly with GH-timetags, like this:

        ...
        $PGHP,1,2014,12,31,23,59,59,784,219,,2190073,1,31*19
        !BSVDM,1,1,,B,13uF31P000Pmw>VPuFwP:jol08S0,0*31
        $PGHP,1,2014,12,31,23,59,59,784,219,,2190047,1,2E*6B
        !BSVDM,1,1,,B,13B3Rf0001PsOdVOleN:h92008S0,0*2E
        ...

- A known set of IP and port number pairs of running Cassandra nodes in the same cluster.

### Importing files
To import the files into Cassandra:

    $ java -jar ais-store-cli-0.3-SNAPSHOT.jar import -i ~/path-to-zipped-ais-files -seeds 192.168.1.37:49153

### Verifying that files are import
A quick test to verify that the AIS data are indeed being imported:

    $ sudo docker exec -it backstabbing_feynman cqlsh
    Connected to Docker Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 2.1.2 | CQL spec 3.2.0 | Native protocol v3]
    Use HELP for help.
    cqlsh> use aisdata;
    cqlsh:aisdata> select * from packets_time;
    <... lots of data rows being showed ...>
