'use strict';
var debug = require('debug')('qp:Workers');
var async = require('neo-async');
var Worker = require('./worker');

var Workers = module.exports = function(queue) {
  var self = this;

  self.disque = queue.disque;

  self.queue = queue;
  self.qp = queue.qp;

  self.workers = [];
  self.acks = [];
  self.nacks = [];
};

Workers.prototype.process = function(num, cb) {
  var self = this;

  for (var i = 0; i < num; i++) {
    self.spawnWorker(cb);
  }
};

Workers.prototype.spawnWorker = function(cb) {
  var self = this;

  debug('spawning worker');

  var w = new Worker(self);
  self.workers.push(w);

  w.on('job', function(job) {
    cb(job, job.done.bind(job));
  });

  w.on('jobDone', function(job) {
    self.ack(job.id);
  });

  var nacks = self.queue.opts.nacks;
  if (nacks) {
    w.on('jobError', function(job) {
      // already gone through maximum nacks, so give up and ack
      if (job.counters.nacks >= nacks) {
        self.ack(job.id);
        return;
      }
      self.nack(job.id);
    });
  }

  w.start();
  return w;
};

Workers.prototype.ack = function(id) {
  var self = this;

  var ackDelay = self.queue.opts.ackDelay;

  if (!ackDelay) {
    var cmd = self.queue.opts.fastAck ? 'fastack' : 'ackjob';
    self.disque[cmd](id);
    return;
  }

  self.acks.push(id);
  if (self._ackDelay) return;
  self._ackDelay = setTimeout(self.runAcks.bind(self), ackDelay);
};

Workers.prototype.nack = function(id) {
  var self = this;

  var ackDelay = self.queue.opts.ackDelay;

  if (!ackDelay) {
    self.disque.nack(id);
    return;
  }

  self.nacks.push(id);
  if (self._nackDelay) return;
  self._nackDelay = setTimeout(self.runNacks.bind(self), ackDelay);
};

Workers.prototype.runAcks = function(cb) {
  var self = this;

  if (!cb) cb = function() {};

  var acks = self.acks;
  self.acks = [];
  clearTimeout(self._ackDelay);
  self._ackDelay = false;

  var cmd = self.queue.opts.fastAck ? 'fastack' : 'ackjob';
  self.disque[cmd](acks, cb);
};

Workers.prototype.runNacks = function(cb) {
  var self = this;

  if (!cb) cb = function() {};

  var nacks = self.nacks;
  self.nacks = [];
  clearTimeout(self._nackDelay);
  self._nackDelay = false;

  self.disque.nack(nacks, cb);
};

Workers.prototype.stop = function(cb) {
  var self = this;

  debug('stopping ' + self.workers.length + ' workers');

  async.each(self.workers, function(w, done) {
    w.stop(done);
  }, function(err) {
    if (err) return cb(err);

    async.parallel([
      self.runAcks.bind(self),
      self.runNacks.bind(self)
    ], cb);
  });
};

Workers.prototype.start = function() {
  var self = this;

  debug('starting ' + self.workers.length + ' workers');

  self.workers.forEach(function(w) {
    w.start();
  });
};

Workers.prototype.pause = function(cb) {
  var self = this;

  debug('pausing ' + self.workers.length + ' workers');

  async.each(self.workers, function(w, done) {
    w.pause(done);
  }, function(err) {
    if (err) return cb(err);

    async.parallel([
      self.runAcks.bind(self),
      self.runNacks.bind(self)
    ], cb);
  });
};


