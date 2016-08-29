
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

	function getLimitCount(resolution, interval, range){
		var resolutionSize = 0;
		switch(resolution){
			case 'rm': // minute
			resolutionSize = 60 * 24;
			break;
			case 'rh': // hour
			if(range=='d')
				resolutionSize = 24;
			else if(range=='w')
				resolutionSize = 24*7;
			break;
			case 'rd': // day
			if(range=='w')
				resolutionSize = 7;
			else if(range=='m')
				resolutionSize = 30;
			break;
		}
		return (resolutionSize/interval);
	}

	function getResolutionConfig(resolution, interval){
		var config = {};
		switch(resolution){
			case 'rm': // minute
				config["Y"] = {$year:"$timestamp"};
				config["M"] = {$month:"$timestamp"};
				config["D"] = {$dayOfMonth:"$timestamp"};
				config["H"] = {$hour:"$timestamp"};

				if(interval==1){
					config["m"] = {$minute:"$timestamp"};
				}else{
					config["m"] = {$subtract:[{$minute:"$timestamp"}, {$mod:[{$minute:"$timestamp"}, interval]}]};
				}
			break;
			case 'rh': // hour
				config["Y"] = {$year:"$timestamp"};
				config["M"] = {$month:"$timestamp"};
				config["D"] = {$dayOfMonth:"$timestamp"};

				if(interval==1){
					config["H"] = {$hour:"$timestamp"};
				}else{
					config["H"] = {$subtract:[{$hour:"$timestamp"}, {$mod:[{$hour:"$timestamp"}, interval]}]};
				}

			break;
			case 'rd': // day
				config["Y"] = {$year:"$timestamp"};
				config["M"] = {$month:"$timestamp"};
				config["D"] = {$dayOfMonth:"$timestamp"};
			break;
		}
		return config;
	}

	function MongoAggreNode(n) {
		RED.nodes.createNode(this, n);
		this.deviceid = n.deviceid;
		this.sensorid = n.sensorid;
		this.resolution = n.resolution;
		this.interval = n.interval;
		this.range = n.range;
		this.mongodb = n.mongodb;
		this.mongoConfig = RED.nodes.getNode(this.mongodb);

		if(!this.mongoConfig){
			this.error(RED._("mongodb.errors.nodbconfig"));
			return;
		}

		var node = this;
		var url = this.mongoConfig.url;
		MongoClient.connect(url, function(err, db) {

			if (err) {
				node.error(err);
				return;
			}

			node.clientDb = db;
			node.on("input",function(msg) {

				var deviceId = node.deviceid;
				var sensorId = node.sensorid;

				var int_interval = parseInt(node.interval);
				var coll = db.collection(node.resolution+deviceId);
				var limit = getLimitCount(node.resolution, int_interval, node.range);

				var matchObj = {};
				matchObj["sensorId"] = sensorId;
				if(msg.payload.start_date && msg.payload.end_date){
					matchObj["timestamp"] = {$gte: new Date(), $lte: new Date()};
				}else{
					if(msg.payload.start_date){
						// var startTime = new Date();
						// var endTime = new Date();
						// endTime.setHours(endTime.getHours() - 2);
						// "timestamp":{$gte: ed, $lte: new Date()}
						matchObj["timestamp"] = {$gte: new Date()};
					}

					if(msg.payload.end_date){
						matchObj["timestamp"] = {$lte: new Date()};
					}
				}

				var aggrs = [];
				aggrs.push({$match: matchObj });
				aggrs.push({$group: 
							{ "_id": getResolutionConfig(node.resolution, int_interval), 
							"mysum": {$sum : "$sum"}, 
							"mycount": {$sum : "$count"}, 
							"vmax": {$max:"$vmax"}, 
							"vmin": {$min:"$vmin"} }
						});

				aggrs.push({$project: {"_id":1, "vavg": {$divide: ["$mysum","$mycount"]}, "vmax":1, "vmin":1}});
				aggrs.push({$sort: {"_id":-1} });
				aggrs.push({$limit: limit });

				console.log('[debug]'+JSON.stringify(aggrs));

				coll.aggregate(aggrs)
					.toArray(function(err, result) {
						if(err){
							node.error(err);
							return;
						}
						var qresult = {payload: {query:matchObj, datas:result}};
						node.send(qresult);
					});
			});

		});

		this.on("close", function() {
			if (this.clientDb) {
				this.clientDb.close();
			}
		});

	}

	RED.nodes.registerType("mongodb aggre", MongoAggreNode);

}
