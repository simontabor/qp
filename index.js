var Queue = require('./src/queue');

var Server = require('./src/server');

var QP = module.exports = function() {
  this.queues = {};
};

QP.prototype.getQueue = function(name) {
  return this.queues[name] || (this.queues[name] = new Queue(name));
};

QP.prototype.createServer = function(name, port) {
  return new Server(name, port);
};
