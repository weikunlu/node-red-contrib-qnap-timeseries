
module.exports = function(RED) {
	"use strict";
	var moment = require('moment');
	var when = require('when');
	var mongo = require('mongodb');
	var MongoClient = mongo.MongoClient;

	function MongoNode(n) {
		RED.nodes.createNode(this,n);
		this.hostname = n.hostname;
		this.port = n.port;
		this.db = n.db;
		this.name = n.name;

		var url = "mongodb://";
		if (this.credentials && this.credentials.user && this.credentials.password) {
			url += this.credentials.user+":"+this.credentials.password+"@";
		}
		url += this.hostname+":"+this.port+"/"+this.db;

		this.url = url;
	}

	RED.nodes.registerType("mongodb",MongoNode,{
		credentials: {
			user: {type:"text"},
			password: {type: "password"}
		}
	});
// --------------- end of MongoNode

	function addSensorData(coll, data){
		var deferred = when.defer();
		coll.insert(data, function(err, item) {
			if (err) {
				deferred.reject('Error:'+ err);
			}else{
				deferred.resolve('add sdata succ');
			}
		}); 
		return deferred.promise;
	}

	function updateSensorReport(coll, date, sensorId, value, sucMsg){
		var deferred = when.defer();
		coll.updateOne(
				{timestamp : date, sensorId : sensorId},
			{
				$inc : { sum : value, count : 1},
				$min : { vmin : value }, $max: { vmax : value }
			},
			{upsert: true},
			function(err, result){
				if (err) {
					deferred.reject('Error:'+ err);
				}else{
					deferred.resolve(sucMsg);
				}
			}
		);
		return deferred.promise;
	}

	function MongoOutNode(n) {
		RED.nodes.createNode(this, n);
		this.mongodb = n.mongodb;
		this.mongoConfig = RED.nodes.getNode(this.mongodb);

		if(!this.mongoConfig){
			this.error(RED._("mongodb.errors.nodbconfig"));
			return;
		}

		var statusTimer;
		var startClearStatusTimer = function(_node){
			if(statusTimer) clearTimeout(statusTimer);
			statusTimer = setTimeout(function(){
				_node.status({});
			}, 1000);
		}

		var node = this;
		var url = this.mongoConfig.url;
		var collection = '';

		MongoClient.connect(url, function(err, db) {
			if (err) {
				node.error(err);
				return;
			}

			node.clientDb = db;
			node.on("input",function(msg) {
				// msg format: {deviceId:<string>, sensorId:<string>, value:<number>}

				if(!msg.payload.deviceId || !msg.payload.sensorId || !msg.payload.value){
					node.error(RED._("mongodb.errors.nocollection"), msg);
					return;
				}

				if(!collection){
					collection = msg.payload.deviceId;
				}else if(collection!=msg.payload.deviceId){
					node.error(RED._("mongodb.errors.diffcollection"), msg);
					return;
				}

				startClearStatusTimer(node);
				this.status({fill:"blue",shape:"dot",text:"saving "+msg.payload.deviceId});

				var deviceId = msg.payload.deviceId;
				var sensorId = msg.payload.sensorId;
				var value = msg.payload.value;

				var coll = db.collection('d'+deviceId);
				var minCollection = db.collection('rm'+deviceId);
				var hrCollection = db.collection('rh'+deviceId);
				var dayCollection = db.collection('rd'+deviceId);

				var timestamp_string = moment.utc().valueOf();
				var timestamp = new Date(timestamp_string);
				var updateMinute = new Date( timestamp.getFullYear(), timestamp.getMonth(), timestamp.getDate(), timestamp.getHours(), timestamp.getMinutes()); 
				var updateHour = new Date( timestamp.getFullYear(), timestamp.getMonth(), timestamp.getDate(), timestamp.getHours()); 
				var updateDaily = new Date( timestamp.getFullYear(), timestamp.getMonth(), timestamp.getDate()); 

				var data = [
					{
						"sensorId":sensorId,
						"value": value,
						"timestamp": new Date(timestamp_string)
					}
				];

				when.all([
					addSensorData(coll, data),
					updateSensorReport(minCollection, updateMinute, sensorId, value, 'update min suc'), 
					updateSensorReport(hrCollection, updateHour, sensorId, value, 'update hr suc'), 
					updateSensorReport(dayCollection, updateDaily, sensorId, value, 'update daily suc')
					]).done(
					function(results){
						//console.log('success:'+ JSON.stringify(results));
					}, 
					function(err1){
						node.error(err1);
					}
				);


			});
		});

		this.on("close", function() {
			if (this.clientDb) {
				this.clientDb.close();
			}
		});
	}



	RED.nodes.registerType("mongodb out", MongoOutNode);
// --------------- end of MongoOutNode

}
