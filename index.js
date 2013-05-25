var Queue = require('./src/queue');

var Server = require('./src/server');
var redis = require('./src/redis');

var QP = module.exports = function() {
  this.queues = {};

  this.redis = redis.client();
};

QP.prototype.getQueue = function(name) {
  return this.queues[name] || (this.queues[name] = new Queue(name));
};

QP.prototype.createServer = function(name) {
  return new Server(name, this);
};

QP.prototype.getQueues = function(cb) {
  this.redis.smembers('qp:job:types', cb);
};
