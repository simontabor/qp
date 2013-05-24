var util = require('util');
var EventEmitter = require('events').EventEmitter;

var redis = require('./redis');

var Worker = module.exports = function(queue) {
  this.redis = redis.createClient();
  this.queue = queue;
};

util.inherits(Worker, EventEmitter);

Worker.prototype.getJob = function(cb) {
  this.redis.blpop('qp:' + this.queue.name + ':jobs', 0, function(e, job) {
    cb(job[1]);
  });
};

Worker.prototype.process = function() {
  var self = this;

  self.getJob(function(job) {
    self.emit('job', job);
  });
};





