 "use strict";
var amqp = require('amqp');

var util = require('util');
var fs = require('fs');
function log(msg) {
	fs.writeSync(1, msg + '\n');
}

var conns = [];
exports.connect = function (url, name, callback) {
	var conn = conns[url] = (conns[url] || amqp.createConnection({url: url}));

	if (conn.isReady)
		connected(conn, name, callback);
	else {
		conn.addListener('ready', readyListener);
		conn.addListener('error', function (err) {
			console.error(err);
			// This is fairly terrible and will bring down the whole app if uncaught, but it's better than
			// silently going into a rabbit hole. The only thing more terrible is that pun. :)
			// We should probably rewrite this and make it an eventEmitter.
			throw err;
		});
	}

	function readyListener() {
		conn.removeListener('ready', readyListener);
		conn.isReady = true;
		connected(conn, name, callback);
	}
};

exports.close = function (url) {
	if(!conns[url]) return;
	conns[url].disconnect();
	delete conns[url];
};

function connected(conn, name, callback) {
	console.log("connected to", name, "on", conn.serverProperties.product);

	conn.exchange(name + 'Xch', {type: 'fanout', durable: true, autoDelete: false}, function (exchange) {

		function publish(message) {
			exchange.publish("msg", message, {
				mandatory: true,
				deliveryMode: 2,
				messageId: process.pid + "-" + Date.now()
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
