var Queue = require('./src/queue');

var Server = require('./src/server');
var redis = require('./src/redis');

var Batch = require('batch');

var QP = module.exports = function(opts) {
  this.queues = {};
  this.opts = opts || {};

  if (this.opts.cleanShutdown) {
    this.cleanShutdown();
  }
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

QP.prototype.stop = function(cb) {
  var batch = new Batch();

  for (var i in this.queues) {
    var q = this.queues[i];
    batch.push(q.stop.bind(q));
  }
  batch.end(cb);
};

QP.prototype.cleanShutdown = function() {
  var self = this;

  ['SIGHUP', 'SIGINT', 'SIGTERM'].forEach(function(sig) {
    process.on(sig, function(){
      console.log('qp caught signal ' + sig);
      self.stop(self.opts.shutdownCB || process.exit);
    });
  });
};
