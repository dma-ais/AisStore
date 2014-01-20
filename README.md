AisStore (Updated 4. July 2013)
-------------------------------------------------------------------------------
AisStore is an online high-available database capable of archiving multiple terabytes of AIS-data with an insert rate of at least 100.000 messages/minute. 
Besides the archiving mechanism is features an AIS-data export tool that is limited only by disk-read speed. 
It also provides simple analytical capabilities based on
time, geography and identity. Furthermore queries based on smaller time intervals,
limited geographical areas, or single ships can be performed in real-time (less than 1 second).


Build Instructions
-------------------------------------------------------------------------------
Prerequisites: Java 1.7 + Maven 3
> git clone git@github.com:cakeframework/cake.git
> cd cake
> mvn install
You can find the command line interface jar to AisStore in ais-store-cli/target/ais-store-cli-xxxx.jar

Source Code Organization
-------------------------------------------------------------------------------
The project is organized in the following directories

ais-store-common     Contains common classes for AisStore
ais-store-raw        Contains the raw-exporter and job implementation that works on raw Cassandra files
ais-store-cli        Contains the all inclusive - command line interface to AisStore


AIS Store Archiver
-------------------------------------------------------------------------------
 


AIS Store Exporter
-------------------------------------------------------------------------------



Deployment at DMA
-------------------------------------------------------------------------------
AisStore is currently deployed at the Danish Maritime Authority.
Where it currently processes around 60.000 messages per minute.
