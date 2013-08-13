var Queue = require('./src/queue');

var Server = require('./src/server');
var redis = require('./src/redis');

var QP = module.exports = function(opts) {
  this.queues = {};
  this.opts = opts || {};
};

QP.prototype.redisClient = function(func) {
  redis.createClient = func;
};

QP.prototype.getQueue = function(name) {
  return this.queues[name] || (this.queues[name] = new Queue(name, this));
};

QP.prototype.createServer = function(name) {
  return new Server(name, this);
};

QP.prototype.getQueues = function(cb) {
  redis.client().smembers('qp:job:types', cb);
};
