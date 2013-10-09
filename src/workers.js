var debug = require('debug')('qp:Workers');

var Batch = require('batch');

var Worker = require('./worker');

var Workers = module.exports = function(queue, red) {
  var self = this;

  this.redis = red;

  this.queue = queue;
  this.qp = queue.qp;

  this.workers = [];

};

Workers.prototype.process = function(opts, cb) {
  var self = this;

  // allow concurrency not to be set
  if (typeof opts == 'function' && !cb) {
    cb = opts;
    opts = {};
  }

  if (typeof opts === 'number') {
    opts = { concurrency: opts };
  }

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

  for (var i = 0; i < (opts.concurrency || 1); i++) {
    this.spawnWorker(opts, cb);
  }

  self.checkRate(opts, cb);

};


Workers.prototype.checkRate = function(opts, cb) {
  var self = this;

  if (self.stopped) return;

  debug('checking rate');

  var diff = Date.now() - self.start;
  var minBound = self.minRate * (diff / self.rateInterval);
  var maxBound = self.maxRate * (diff / self.rateInterval);

  debug(self.workers.length + ' workers');

  if (self.processed > maxBound) {
    var ahead = self.processed - maxBound;
    self.pause();

    var timeout = ahead * (self.rateInterval / self.maxRate);

    // sanity for small maxRates
    if (timeout > self.rateInterval) timeout = self.rateInterval - diff;

    debug('too fast - pausing for ' + timeout + 'ms');
    debug(self.processed + ' / ' + maxBound + ' done');

    for (var i = 0; i < ahead; i++) {
      var w = self.workers.pop();
      if (!w) continue;
      w.stop();
    }

    setTimeout(self.checkRate.bind(self, opts, cb), timeout);
    return;
  }

  // needs to go faster!
  if (self.processed < minBound) {
    var behind = minBound - self.processed;

    debug('too slow, ' + behind +' jobs behind');

    self.paused = false;

    if (self.workers.length > self.maxProcessing) {
      debug('already spawned enough processes');
    } else {
      var numNew = Math.ceil(behind);
      if (numNew + self.workers.length > self.maxProcessing) {
        numNew = self.maxProcessing - self.workers.length;
      }
      debug('spawning ' + numNew + ' workers');
      for (var i = 0; i < numNew; i++) self.spawnWorker(opts, cb);
    }
  }

  // we have no workers, start one up
  if (!self.workers.length) self.spawnWorker(opts, cb);

  // unpause any paused workers
  for (var i = 0; i < self.workers.length; i++) {
    if (self.workers[i].paused) {
      self.workers[i].start();
    }
  }

  setTimeout(self.checkRate.bind(self, opts, cb), self.checkInterval);

};

Workers.prototype.spawnWorker = function(opts, cb) {
  var self = this;

  debug('spawning worker');

  var w;
  if (this.qp.opts.noBlock) {
    w = new Worker(this, opts, this.redis);
  } else {
    w = new Worker(this, opts);
  }

  this.workers.push(w);

  w.on('job', function(job) {

    // option not to fetch info on processing
    if (self.qp.opts.noInfo) {
      job.setState('active');
      cb(job, job.done.bind(job));
      return;
    }

    job.getInfo(function() {

      // if theres a timeout - set it up
      if (job._timeout) {
        job.__timeout = setTimeout(function() {
          job.done('timeout');
        }, job._timeout);
      }

      job.setState('active');
      cb(job, job.done.bind(job));
    });

  });

  w.start();
};

Workers.prototype.stop = function(cb) {
  this.stopped = true;

  debug('stopping ' + this.workers.length + ' workers');

  var batch = new Batch();
  for (var i = 0; i < this.workers.length; i++) {
    var w = this.workers[i];
    batch.push(w.stop.bind(w));
  }
  batch.end(cb);

};

Workers.prototype.pause = function(cb) {
  this.paused = true;

  debug('pausing');

  var batch = new Batch();
  for (var i = 0; i < this.workers.length; i++) {
    var w = this.workers[i];
    batch.push(w.pause.bind(w));
  }
  batch.end(cb);

};
