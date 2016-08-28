node-red-contrib-qnap-timeseries
=====================

A <a href="http://nodered.org" target="_new">Node-RED</a> node to save time-series data in a MongoDB database.


Pre-requisite
-------------

To run this you need a local MongoDB server running. For details see
<a href="https://www.mongodb.org/" target="_new">the MongoDB site</a>.

Install
-------

Run the following command in your Node-RED user directory - typically `~/.node-red`

        npm install node-red-contrib-qnap-timeseries


Usage
-----

Nodes to save and retrieve data in a local MongoDB instance.

### Input

*Find* queries a collection using the `msg.payload` as the date query statement.

Optionally, you may also set

- a `msg.payload.start_date` object to constrain the returned data set,
- a `msg.payload.end_date` object to constrain the returned data set

Default query using historic configuration provided to aggregation method of collections.


### Output

A simple MongoDB output node. Can save and update objects from a chosen collection to time-series format.