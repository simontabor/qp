'use strict';
var debug = require('debug')('qp:RateControlledWorkers');
var async = require('neo-async');
var Workers = require('./Workers');
var util = require('util');

var RateControlledWorkers = module.exports = function() {
  var self = this;

  Workers.apply(self, arguments);
  self.jobsComplete = 0;

  self.opts = {
    start: 1,
    spawn: 1,
    max: Infinity,
    min: 1,

    jobs: 1,
    interval: 1000
  };
};

util.inherits(RateControlledWorkers, Workers);

RateControlledWorkers.prototype.setOpts = function(opts) {
  var self = this;
  for (var i in opts) self.opts[i] = opts[i];
};

RateControlledWorkers.prototype.process = function(opts, cb) {
  var self = this;
  self.setOpts(opts);
  self.jobCB = cb;

  var num = self.opts.start;
  for (var i = 0; i < num; i++) self.spawnWorker();

  self.checkRateTimeout();
};

RateControlledWorkers.prototype.start = function() {
  var self = this;
  Workers.prototype.start.call(self);
  self.checkRateTimeout();
};

RateControlledWorkers.prototype.checkRateTimeout = function() {
  var self = this;
  self.jobsComplete = 0;
  clearTimeout(self.checkTimeout); // just in case
  self.checkTimeout = setTimeout(self.checkRate.bind(self), self.opts.interval);
};

RateControlledWorkers.prototype.spawnWorker = function() {
  var self = this;

  debug('spawning worker');

  var w = Workers.prototype.spawnWorker.call(self, self.jobCB);
  // w.on('job', function() {

  // });

  var done = function() {
    self.jobsComplete++;
    if (self.jobsComplete >= self.opts.jobs) {
      w.pause();
      if (!self.goneOver) self.goneOver = Date.now();
    }
  };
  w.on('jobDone', done);
  w.on('jobError', done);
};

RateControlledWorkers.prototype.killWorker = function() {
  var self = this;

  debug('killing worker');

  var w = self.workers.pop();
  w.stop();
};

RateControlledWorkers.prototype.checkRate = function() {
  var self = this;

  debug('checking rate');

  var complete = self.jobsComplete;
  if (self.goneOver) {
    // pretend we were processing the whole time, how many jobs would have been done?
    var timeToOver = self.goneOver - (Date.now() - self.opts.interval);
    complete *= self.opts.interval / timeToOver;
    self.goneOver = false;
  }

  var ratio = self.opts.jobs / complete;
  var desiredWorkers = Math.round(self.workers.length * ratio);

  if (desiredWorkers < self.opts.min) desiredWorkers = self.opts.min;
  if (desiredWorkers > self.opts.max) desiredWorkers = self.opts.max;

  var diff = desiredWorkers - self.workers.length;
  var maxDiff = self.opts.spawn;

  // check if any workers are waiting (e.g. there are no jobs so there's no point spawning more workers)
  var workersWaiting = self.workers.some(function(w) {
    return w.waiting;
  });

  if (!workersWaiting && diff > 0) {
    if (diff > maxDiff) diff = maxDiff;
    for (var i = 0; i < diff; i++) self.spawnWorker();
  }
  if (diff < 0) {
    var kill = -diff;
    if (kill < maxDiff) kill = maxDiff;
    for (var i = 0; i < kill; i++) self.killWorker();
  }

  self.start();
};

RateControlledWorkers.prototype.stop = function(cb) {
  var self = this;

  clearTimeout(self.checkTimeout);

  Workers.prototype.stop.call(self, function(err) {
    if (err) return cb(err);

    cb(err);
  });
};

RateControlledWorkers.prototype.pause = function(cb) {
  var self = this;

  clearTimeout(self.checkTimeout);

  Workers.prototype.pause.call(self, function(err) {
    if (err) return cb(err);

    cb(err);
  });
};
