 "use strict";
var amqp = require('amqp');
var uuid = require('uuid');

var util = require('util');
var fs = require('fs');

exports.log = console;

var conns = [];

exports.connectEx = function (options, callback) {
	options = options || {};
	var conn = conns[options.url] = (conns[options.url] || amqp.createConnection({url: options.url, heartbeat: options.heartbeat}));

	if (conn.isReady)
		connected(conn, options.name, callback);
	else {
		conn.addListener('ready', readyListener);
		conn.addListener('error', function (err) {
			exports.log.error('amqp error', err);
			// This is fairly terrible and will bring down the whole app if uncaught, but it's better than
			// silently going into a rabbit hole. The only thing more terrible is that pun. :)
			// We should probably rewrite this and make it an eventEmitter.
			throw err;
		});
		conn.addListener('heartbeat', function () {
			exports.log.debug('amqp heartbeat');
		});
	}

	function readyListener() {
		conn.removeListener('ready', readyListener);
		conn.isReady = true;
		connected(conn, options.name, callback);
	}
};

 // backward compat connect method
exports.connect = function (url, name, callback) {
	return exports.connectEx({
		url,
		name
	}, callback);
};

exports.close = function (url) {
	if(!conns[url]) return;
	conns[url].end();
	delete conns[url];
};

function connected(conn, name, callback) {
	exports.log.info("connected to", name, "on", conn.serverProperties.product);

	conn.exchange(name + 'Xch', {type: 'fanout', durable: true, autoDelete: false}, function (exchange) {

		function publish(message) {
			exchange.publish("msg", message, {
				mandatory: true,
				deliveryMode: 2,
				messageId: process.pid + "-" + uuid.v4()
			});
		}

		function subscribeToWorkQueue(cb, fetchCount) {
			fetchCount = fetchCount || 1;
			conn.queue(name + 'Q', {durable: true, autoDelete: false}, function (q) {
				q.bind(exchange, "*");
				q.subscribe({ ack: true, prefetchCount: fetchCount }, function (json, headers, deliveryInfo, msg) {
					cb({
						data: json,
						headers: headers,
						deliveryInfo: deliveryInfo,
						ack: function () {
							msg.acknowledge();
						}
					});
				});
			});
		}

		function subscribeToFanoutQueue(cb) {
			conn.queue(name + 'Q-' + process.pid + '-' + Math.round(100000 * Math.random()), {durable: false, exclusive: true}, function (q) {
				q.bind(exchange, "*");
				q.subscribe({ ack: true }, function (json, headers, deliveryInfo, msg) {
					cb({
						data: json,
						headers: headers,
						deliveryInfo: deliveryInfo,
						ack: function () {
							msg.acknowledge();
						}
					});
				});
			});
		}

		callback({
			publish: publish,
			subscribeToWorkQueue: subscribeToWorkQueue,
			subscribeToFanoutQueue: subscribeToFanoutQueue
		});
	});
}
