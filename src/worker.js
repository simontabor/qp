'use strict';
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var async = require('neo-async');
var Disque = require('disque-client');

var debug = require('debug')('qp:Worker');

var Worker = module.exports = function(workers) {
  var self = this;

  self.workers = workers;
  self.queue = workers.queue;
  self.qp = self.queue.qp;
  self.disque = workers.disque;

  // if we're going to be using HANG, create a new client so we don't block
  // everything else from happening
  if (this.queue.opts.getTimeout) {
    self.getDisque = new Disque(self.qp.opts.disque);
  } else {
    self.getDisque = self.disque;
  }

  self.cmd = self.getCmd();
};

util.inherits(Worker, EventEmitter);

Worker.prototype.getCmd = function() {
  var cmd = [];

  var timeout = this.queue.opts.getTimeout;
  if (timeout) {
    cmd.push('TIMEOUT', timeout);
  } else {
    cmd.push('NOHANG');
  }

  var count = this.queue.opts.count;
  if (count) cmd.push('COUNT', count);

  cmd.push('WITHCOUNTERS');

  cmd.push('FROM', this.queue.name);
  return cmd;
};

Worker.prototype.getJobs = function(cb) {
  var self = this;

  debug('getting job');

  self.waiting = true;

  self.getDisque.getjob(self.cmd, function(err, jobs) {
    if (err || jobs) {
      self.waiting = false;
      return cb(err, jobs);
    }

    // stop quickly if we're meant to stop or pause
    if (self.stopping || self.pausing) return self.process();

    debug('no job, retry');
    setTimeout(function() {
      self.process();
    }, self.queue.opts.getInterval);
  });
};

Worker.prototype.process = function() {
  var self = this;

  if (self.stopping) {
    self.processing = self.waiting = self.working = false;
    debug('worker stopped, not processing');
    self.emit('stopped');
    return;
  }

  if (self.pausing) {
    self.processing = self.waiting = self.working = false;
    debug('worker paused, not processing');
    self.emit('paused');
    return;
  }

  self.processing = true;

  self.getJobs(function(err, jobs) {
    if (err) {
      debug('error getting jobs');
      self.emit('error', err);
      return;
    }

    debug('got jobs');

    self.working = true;

    async.each(jobs, function(j, done) {
      debug('got job');

      var job = self.queue.create(self.queue.decode(j[2]));
      job.id = j[1];
      job.counters = {
        nacks: j[4],
        additional_deliveries: j[6]
      };
      job.worker = self;

      job.once('done', function() {
        debug('job complete');
        self.emit('jobDone', job);
        done();
      });
      job.once('error', function() {
        debug('job error');
        self.emit('jobError', job);
        done();
      });

      self.emit('job', job);
    }, function() {
      self.working = false;
      self.process();
    });
  });
};

Worker.prototype.start = function() {
  var self = this;

  // no starting after stopped
  if (self.stopped || self.stopping) {
    debug('start refused - stopped');
    return;
  }

  self.paused = false;
  if (self.pausing) {
    debug('cancelling pause');
    self.pausing = false;
    for (var i = 0; i < self._pauseCBQ.length; i++) {
      self._pauseCBQ[i](new Error('restarted'));
    }
    self._pauseCBQ = null;
  }

  if (!self.processing) self.process();
};

Worker.prototype.stop = function(cb) {
  var self = this;

  if (!cb) cb = function() {};

  if (self.stopped) return cb();
  if (self.stopping) return self._stopCBQ.push(cb);

  debug('stopping');
  self.stopping = true;
  self._stopCBQ = [ cb ];

  var done = function() {
    debug('stopped');

    self.stopping = false;
    self.stopped = true;

    var runCBs = function() {
      for (var i = 0; i < self._stopCBQ.length; i++) {
        self._stopCBQ[i]();
      }
    };

    if (!self.queue.opts.getTimeout) return runCBs();
    self.getDisque.quit(runCBs);
  };

  if (!self.working && !self.waiting) return done();

  self.once('stopped', done);
};

Worker.prototype.pause = function(cb) {
  var self = this;

  if (!cb) cb = function() {};

  if (self.paused) return cb();
  if (self.pausing) return self._pauseCBQ.push(cb);

  debug('pausing');
  self.pausing = true;
  self._pauseCBQ = [ cb ];

  var done = function() {
    debug('paused');

    self.pausing = false;
    self.paused = true;

    for (var i = 0; i < self._pauseCBQ.length; i++) {
      self._pauseCBQ[i]();
    }

    self._pauseCBQ = null;
  };

  if (!self.working && !self.waiting) return done();

  self.once('paused', done);
};
