var util = require('util');
var EventEmitter = require('events').EventEmitter;

var debug = require('debug')('qp:Worker');

var redis = require('./redis');

var Worker = module.exports = function(queue, opts, red) {
  var self = this;

  this.opts = opts;

  this.redis = red || redis.createClient();
  this.queue = queue;

  this.processed = 0;
  this.processing = 0;
  this.start = Date.now();

  this.minRate = opts.minRate || 0;
  this.maxRate = opts.maxRate || Infinity;
  this.maxProcessing = opts.maxProcessing || this.maxRate * 2;
  this.rateInterval = opts.rateInterval || 1000;
  this.checkInterval = opts.checkInterval || this.rateInterval / 10;

  setInterval(function() {
    self.start = Date.now();
    self.processed = 0;
  }, this.rateInterval);

  self.checkRate();


};

util.inherits(Worker, EventEmitter);

Worker.prototype.checkRate = function() {
  var self = this;

  if (self.stopped) return;

  debug('checking rate');

  var minBound = self.minRate * ((Date.now() - self.start) / self.rateInterval);
  var maxBound = self.maxRate * ((Date.now() - self.start) / self.rateInterval);

  if (self.processed > maxBound) {
    debug('too fast - pausing');
    var ahead = self.processed - maxBound;
    self.pause();

    // check again when we'll be back behind the maxbound
    setTimeout(self.checkRate.bind(self), (self.processed - maxBound) * (self.maxRate / self.rateInterval));
    return;
  }

  // needs to go faster!
  if (self.processed < minBound) {
    debug('too slow');
    var behind = minBound - self.processed;
    self.paused = false;

    if (self.processing > self.maxProcessing) {
      debug('already spawned enough processes');
    } else {
      debug('spawning workers');
      for (var i = 0; i < behind; i++) self.process();
    }
  }

  setTimeout(self.checkRate.bind(self), self.checkInterval);

};

Worker.prototype.getBlockingJob = function(cb) {
  debug('getting blocking job');

  this.redis.blpop('qp:' + this.queue.name + ':jobs', 0, function(e, job) {
    cb(e, job && job[1]);
  });
};

Worker.prototype.getNonBlockingJob = function(cb) {
  var self = this;

  debug('getting non-blocking job');

  this.redis.lpop('qp:' + this.queue.name + ':jobs', function(err, job) {
    if (err || !job) {
      debug('no job, retry');
      setTimeout(function() {
        self.process();
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

  if (self.stopped) {
    debug('worker stopped, not processing');
    if (!self.processing) self.emit('stopped');
    return;
  }

  if (self.paused) {
    debug('worker paused, not processing');
    self.emit('paused');
    return;
  }

  self.processing++;

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
      self.processed++;
      self.processing--;

      self.working = false;
      self.process();
    });
  });
};

Worker.prototype.stop = function(cb) {
  this.stopped = true;

  if (!cb) cb = function(){};

  if (!this.working) return cb();

  this.once('stopped', cb);
};

Worker.prototype.pause = function(cb) {
  this.paused = true;

  if (!cb) cb = function(){};

  if (!this.working) return cb();

  this.once('paused', cb);
};
