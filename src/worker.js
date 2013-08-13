var util = require('util');
var EventEmitter = require('events').EventEmitter;

var redis = require('./redis');

var Worker = module.exports = function(queue, red) {
  this.redis = red || redis.createClient();
  this.queue = queue;
};

util.inherits(Worker, EventEmitter);

Worker.prototype.getBlockingJob = function(cb) {
  this.redis.blpop('qp:' + this.queue.name + ':jobs', 0, function(e, job) {
    cb(e, job && job[1]);
  });
};

Worker.prototype.getNonBlockingJob = function(cb) {
  var self = this;

  this.redis.lpop('qp:' + this.queue.name + ':jobs', function(err, job) {
    if (err || !job) {
      setTimeout(function() {
        self.getNonBlockingJob(cb);
      }, self.queue.qp.opts.checkInterval || 200);
      return;
    }
    cb(err, job);
  });
};

Worker.prototype.getJob = function(cb) {
  if (!this.queue.qp.opts.noBlock) {
    this.getBlockingJob(cb);
  } else {
    this.getNonBlockingJob(cb);
  }
};

Worker.prototype.process = function() {
  var self = this;

  self.getJob(function(err, job) {
    if (err) {
      self.emit('error', err);
      return;
    }
    self.emit('job', job);
  });
};
