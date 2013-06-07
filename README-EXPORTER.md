AisStore
-------------------------------------------------------------------------------
AisStore is an online high-available database capable of archiving multiple terabytes of AIS-data with an insert rate of at least 100.000 messages/minute. 
Besides the archiving mechanism is features an AIS-data export tool that is limited only by disk-read speed. 
It also provides simple analytical capabilities based on time, geography and identity. Furthermore queries based on smaller time intervals,
limited geographical areas, or single ships can be performed in real-time (less than 1 second).

Build Instructions
-------------------------------------------------------------------------------
Prerequisites: Java 1.7 + Maven 3
> git clone git@github.com:cakeframework/cake.git
> cd cake
> mvn install
You can find the resulting jar file as singlejar/target/cake-xxxx.jar


Source Code Organization
-------------------------------------------------------------------------------
The project is organized in the following directories

ais-store-common     Contains common classes for AisStore
ais-store-archiver   Contains the archiver responsible for storing AIS-data
ais-store-exporter   Contains the bulk AIS-data command line exporter
ais-store-web        Contains the REST-based query front end



AIS Store Archiver
-------------------------------------------------------------------------------

AIS Store Exporter
-------------------------------------------------------------------------------

AIS Store Web
-------------------------------------------------------------------------------
The main responsibility of the web module is to provide a REST based query interface to the data in AisStore.

How to start it.



Current status
========
The code needs to be cleanup. It was just a prototype being checked in






Deployment at DMA
-------------------------------------------------------------------------------
AisStore is currently deployed at the Danish Maritime Authority.
While Cassandra is normally used wi
It is running on a  

While AisStore should be cassandra makes no assumption about the hardware.
