AisWeb
========

REST
=========
AisWeb is a simple REST based webfrontend to access current and historic Ais data.
It allows filtering of ais data based on time, mmsi numbers and area of interest.
Futhermore it supports output formats such as CSV, JSON, raw format or XML.

Getting Started
===============
Lets jump right in.

A query for getting the last 30 minutes of ais acivity for a single vessel is as simple as invoking
http://10.10.5.202:8080/raw?mmsi=123123123&interval=PT30

The output format is the raw data as received by DMA. But you can choose to get the data either as XML or JSON 
by changing the base url to 
http://10.10.5.202:8080/xml?mmsi=123123123&interval=PT30
or
http://10.10.5.202:8080/json?mmsi=123123123&interval=PT30
respectively

The data for all 3 examples are the same. It is just the output format that is different.

If you want data for more vessels you can just add each mmsi number as an additional query parameter
http://10.10.5.202:8080/raw?mmsi=123123123&interval=PT30&mmsi=xxxxx&mmsi=yyyyy

You can also omit the interval parameter in which case you will get ALL the ais data ever received
for the specified mmsi number.
http://10.10.5.202:8080/raw?mmsi=123123123
As a general rule you always to specify an interval when querying to reduce the amount of data you receive.

Filtering
========
To restrict the number of data returned a number of different filters can be applied

mmsi: Restricts the result to one or more mmsi numbers
interval: Restricts the time interval for which to return data based on a ISO 8601
messageType: Restricts the type of Ais messages that will be returned. valid numbers are:



OutputFormats
========
A number of different output formats are supported:
raw   : Outputs the raw received message including all headers
csv   : Outputs the recieved message as a csv file
json  : Output packets as a JSON message
xml   : Output packets as a XML message




Examples
=========
Future:
area:
duplicateFilter:
downsampler:
source: source filter
limits:
timeout:1h 
Data is always sorted by time ascending. if you want newest data first use &reserve=true
reverse
/aisdata/

JSON
XML
raw
table

