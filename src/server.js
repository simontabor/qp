var express = require('express');
var socketio = require('socket.io');
var Batch = require('batch');

var redis = require('./redis');

var Server = module.exports = function(name, QP) {
  this.name = name || 'QP';

  this.QP = QP;

  this.app = express();
  this.app.set('view engine', 'ejs');

  this.server = require('http').createServer(this.app);
  this.io = socketio.listen(this.server);

  // new client for pubsub
  this.redis = redis.pubsub();

  this.setupRoutes();
  this.startWS();

  // hold all websockets together so we can send to all
  this.sockets = [];

  this.queueSockets = {};



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
        res.render(__dirname + '/app/templates/home.ejs', { title: 'queues', name: self.name, queues: queueData });

      });
    });
  });

  app.get('/:queue', function(req, res) {

    var queue = req.params.queue;
    var q = self.QP.getQueue(queue);
    var queueData = {};

    q.getJobs('completed', 0, 5, function(e, jobs) {
      console.log(jobs);
    });
    self.getQueueData(queue, function(e, data) {


      queueData.nums = data;
      res.render(__dirname + '/app/templates/queue.ejs', { title: queue, name: self.name, queue: queue, queueData: queueData });

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
  if (type === 'state') {
    this.getQueueData(data.queue, function(e, r) {
      data.queueInfo = r;
      cb(data);
    });
    return;
  }

  cb(data);
};

Server.prototype.emit = function(name, data) {
  this.io.sockets.emit(name, data);
};

Server.prototype.startWS = function(port) {
  var self = this;

  var io = this.io;
  io.set('log level', 0);

  io.sockets.on('connection', function (socket) {
    self.sockets.push(socket);

    self.subscribe();

    socket.on('disconnect', function() {

      // remove the socket
      self.sockets.splice(self.sockets.indexOf(socket),1);
      if (!self.sockets.length) self.unsubscribe();
    });
  });

  io.of('/queue').on('connection', function(socket) {
    console.log('qcon');

    var sockData;

    var uncon = function() {
      if (!sockData) return;
      // nothing stored
      if (!self.queueSockets[sockData.queue] || !self.queueSockets[sockData.queue].length) return;

      // remove the socket
      self.queueSockets[sockData.queue].splice(self.queueSockets[sockData.queue].indexOf(socket),1);
    };

    socket.on('init', function(data) {

      // just in case
      uncon();

      sockData = data;
      if (!self.queueSockets[data.queue]) self.queueSockets[data.queue] = [];

      self.queueSockets[data.queue].push(socket);
    });

    socket.on('disconnect', uncon);


  });

};

Server.prototype.subscribe = function() {
  var self = this;

  if (this.subscribed) return;

  this.subscribed = true;

  self.redis.subscribe('qp:events');
  self.redis.on('message', function(key, msg) {

    // if no connections, don't do anything
    if (!self.sockets.length) return;

    msg = JSON.parse(msg);

    if (self.queueSockets[msg.queue] && self.queueSockets[msg.queue].length) {
      console.log('grab data + emit to queue');
    }

    self.getAdditionalData(msg.type, msg, function(msg) {
      self.emit(msg.type, msg);
    });
  });
};

Server.prototype.unsubscribe = function() {
  this.redis.unsubscribe('qp:events');
  this.subscribed = false;
};
