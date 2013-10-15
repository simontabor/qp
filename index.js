var Queue = require('./src/queue');
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

QP.prototype.getQueue = function(name, opts) {
  var q = this.queues[name] || (this.queues[name] = new Queue(this, name));

  // this will overwrite the queue's options if it already exists and if new ones are specified
  q.opts = opts || q.opts || {};
  return q;
};

QP.prototype.createServer = function(name, port) {
  // this will throw if qp-server isnt installed
  var Server = require('qp-server');

  var server = new Server(this, name);
  if (port) server.listen(port);

  return server;
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
