var express = require('express');
var socketio = require('socket.io');
var Batch = require('batch');

var redis = require('./redis');

var Server = module.exports = function(name, QP) {
  this.name = name || 'QP';

  this.QP = QP;

  this.app = express();
  this.app.set('view engine', 'ejs');

  // new client for pubsub
  this.redis = redis.pubsub();

  this.setupRoutes();

  // hold all websockets together so we can send to all
  this.sockets = [];

};

Server.prototype.setupRoutes = function() {
  var self = this;

  var app = this.app;

  app.get('/', function(req, res) {
    self.QP.getQueues(function(e, qs) {

      var queueData = [];

      var batch = new Batch();

      qs.forEach(function(q) {

        batch.push(function(done) {

          self.getQueueData(q, function(e, data) {
            queueData.push({
              name: q,
              data: data
            });
            done();
          });
        });

      });
      batch.end(function() {
        console.log(queueData);
        res.render(__dirname + '/app/templates/home.ejs', { name: self.name, queues: queueData });

      });
    });
  });

};

Server.prototype.getQueueData = function(queue, cb) {
  var self = this;

  queue = self.QP.getQueue(queue);
  queue.numJobs(['inactive', 'active', 'completed'], function(e, data) {
    cb(e, data);
  });
};

Server.prototype.getAdditionalData = function(type, data, cb) {
  switch(type) {
    case 'state':
      this.getQueueData(data.queue, function(e, r) {
        data.queueInfo = r;
        cb(data);
      });
      break;
    default:
      cb(data);
  }
};

Server.prototype.emit = function(name, data) {
  for (var i = 0; i < this.sockets.length; i++) {
    this.sockets[i].emit(name, data);
  }
};

Server.prototype.startWS = function(port) {
  if (!port) return;

  var self = this;

  var io = this.io = socketio.listen(port);
  io.set('log level', 0);

  io.sockets.on('connection', function (socket) {
    self.sockets.push(socket);

    if (!self.subscribed) self.subscribe();

    socket.on('disconnect', function() {

      // remove the socket
      self.sockets.splice(self.sockets.indexOf(socket),1);

      if (!self.sockets.length) self.unsubscribe();
    });
  });

};

Server.prototype.subscribe = function() {
  var self = this;

  this.subscribed = true;

  self.redis.subscribe('qp:events');
  self.redis.on('message', function(key, msg) {

    // if no connections, don't do anything
    if (!self.sockets.length) return;

    msg = JSON.parse(msg);

    self.getAdditionalData(msg.type, msg, function(msg) {
      self.emit(msg.type, msg);
    });
  });
};

Server.prototype.unsubscribe = function() {
  this.redis.unsubscribe('qp:events');
  this.subscribed = false;
};
