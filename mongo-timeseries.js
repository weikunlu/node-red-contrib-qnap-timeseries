
module.exports = function(RED) {
	"use strict";
	var moment = require('moment');
	var when = require('when');
	var mongo = require('mongodb');

	function MongoOutNode(n) {
		RED.nodes.createNode(this, n);
		
	}

	RED.nodes.registerType("mongodb out", MongoOutNode);


}
