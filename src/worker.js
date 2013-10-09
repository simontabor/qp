var util = require('util');
var EventEmitter = require('events').EventEmitter;

var debug = require('debug')('qp:Worker');

var redis = require('./redis');

var Worker = module.exports = function(workers, opts, red) {
  var self = this;

  this.opts = opts;

  this.redis = red || redis.createClient();
  this.workers = workers;
  this.queue = workers.queue;
  this.qp = this.queue.qp;

};

util.inherits(Worker, EventEmitter);

Worker.prototype.getBlockingJob = function(cb) {
  var self = this;

  debug('getting blocking job');

  self.waiting = true;
  this.redis.blpop('qp:' + this.queue.name + ':jobs', 0, function(e, job) {
    self.waiting = false;
    cb(e, job && job[1]);
  });
};

Worker.prototype.getNonBlockingJob = function(cb) {
  var self = this;

  debug('getting non-blocking job');

  this.redis.lpop('qp:' + this.queue.name + ':jobs', function(err, job) {
    if (err || !job) {
      self.waiting = true;
      debug('no job, retry');
      setTimeout(function() {
        self.process();
      }, self.queue.qp.opts.checkInterval || 200);
      return;
    }

    self.waiting = false;
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

  if (self.stopped) {
    self.processing = false;
    debug('worker stopped, not processing');
    self.emit('stopped');
    return;
  }

  if (self.paused) {
    self.processing = false;
    debug('worker paused, not processing');
    self.emit('paused');
    return;
  }

  self.processing = true;

  self.getJob(function(err, jobID) {
    if (err) {
      debug('error getting job');
      self.emit('error', err);
      return;
    }
    debug('got job');

    var job = self.queue.create();
    job.id = jobID;
    job.worker = self;
    job._saved = true;

    self.working = true;

    self.emit('job', job);

    job.once('done', function() {
      debug('job complete');
      self.workers.processed++;

      self.working = false;
      self.process();
    });
  });
};

Worker.prototype.start = function() {
  this.paused = false;

  if (!this.processing) this.process();
};

Worker.prototype.stop = function(cb) {
  if (this.stopped) return;

  debug('stopping');

  if (!cb) cb = function(){};

  if (!this.working || this.stopped) return cb();

  this.stopped = true;

  this.once('stopped', cb);
};

Worker.prototype.pause = function(cb) {

  debug('pausing');

  if (!cb) cb = function(){};

  if (!this.working || this.paused) return cb();

  this.paused = true;

  this.once('paused', cb);
};
