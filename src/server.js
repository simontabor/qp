var express = require('express');
var socketio = require('socket.io');

var redis = require('./redis');


var Server = module.exports = function() {
  this.name = 'QP';
  this.app = express();
  this.app.set('view engine', 'ejs');

  // new client for pubsub
  this.redis = redis.createClient();

  this.setupRoutes();
  this.subscribe();

  // hold all websockets together so we can send to all
  this.sockets = [];

};

Server.prototype.setupRoutes = function() {
  var self = this;

  var app = this.app;

  app.get('/', function(req, res) {
    res.render(__dirname + '/app/templates/home.ejs', { name: self.name });
  });

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

    socket.on('disconnect', function() {

      // remove the socket
      self.sockets.splice(self.sockets.indexOf(socket),1);
    });
  });

};

Server.prototype.subscribe = function() {
  var self = this;

  self.redis.subscribe('qp:events');
  self.redis.on('message', function(key, msg) {

    // if no connections, don't do anything
    if (!self.sockets.length) return;

    msg = JSON.parse(msg);

    self.emit('test', msg);
  });
};
