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

  var diff = Date.now() - self.start;
  var minBound = self.minRate * (diff / self.rateInterval);
  var maxBound = self.maxRate * (diff / self.rateInterval);

  if (self.processed > maxBound) {
    var ahead = self.processed - maxBound;
    self.pause();

    var timeout = ahead * (self.rateInterval / self.maxRate);

    // sanity for small maxRates
    if (timeout > self.rateInterval) timeout = self.rateInterval - diff;

    debug('too fast - pausing for ' + timeout + 'ms');
    debug(self.processed + ' / ' + maxBound + ' done');

    setTimeout(self.checkRate.bind(self), timeout);
    return;
  }

  // needs to go faster!
  if (self.processed < minBound) {
    var behind = minBound - self.processed;

    debug('too slow, ' + behind +' jobs behind');

    self.paused = false;

    if (self.processing > self.maxProcessing) {
      debug('already spawned enough processes');
    } else {
      var numNew = Math.ceil(behind);
      debug('spawning ' + numNew + ' workers');
      for (var i = 0; i < numNew; i++) self.process();
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

  if (self.processing > self.maxProcessing) {
    debug('too many processing, stopping');
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
