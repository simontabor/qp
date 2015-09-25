'use strict';
var async = require('neo-async');
var Job = require('./job');
var Workers = require('./Workers');
var RateWorkers = require('./RateWorkers');

var debug = require('debug')('qp:Queue');

var Queue = module.exports = function(qp, name) {
  var self = this;
  self.name = name;
  self.disque = qp.disque;
  self.qp = qp;
  self.workers = [];

  self.opts = {
    getTimeout: 0,
    getInterval: 200,
    nacks: 1,
    ackDelay: 0,
    timeout: 0,
    replicate: 0,
    delay: 0,
    retry: 0,
    ttl: 0,
    maxLen: Infinity,
    async: false,
    fastAck: false,
    count: 1,
    encode: JSON.stringify,
    decode: JSON.parse
  };

  self.setOpts(self.qp.opts);
  // qp.getQueue will set opts with the queue options
};

Queue.prototype.setOpts = function(opts) {
  var self = this;
  for (var i in opts) self.opts[i] = opts[i];
};

Queue.prototype.create = Queue.prototype.createJob = function(data, opts) {
  var job = new Job(this, data, opts);
  return job;
};

Queue.prototype.saveJob = function(job, cb) {
  var self = this;
  if (!(job instanceof Job)) job = self.create(job);
  job.save(cb);
};

Queue.prototype.saveJobs = function(jobs, limit, cb) {
  var self = this;
  if (!cb && typeof limit === 'function') {
    cb = limit;
    limit = false;
  }

  // no limit, so let's make it essentially unlimited (all jobs at once)
  if (!limit) limit = jobs.length;
  async.eachLimit(jobs, limit, self.saveJob.bind(self), cb);
};

Queue.prototype.process = function(num, cb) {
  if (!cb && typeof num === 'function') {
    cb = num;
    num = 1;
  }

  var workers = new Workers(this);
  this.workers.push(workers);

  workers.process(num, cb);
  return workers;
};

Queue.prototype.rateProcess = function(opts, cb) {
  if (!cb && typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  var workers = new RateWorkers(this);
  this.workers.push(workers);

  workers.process(opts, cb);
  return workers;
};

Queue.prototype.getLength = function(cb) {
  var self = this;

  self.disque.qlen(self.name, cb);
};

Queue.prototype.removeJobs = function(jobs, cb) {
  var self = this;

  var ids = jobs.map(function(j) {
    if (j instanceof Job) return j.id;
    return j;
  });

  self.disque.dequeue(ids, cb);
};

Queue.prototype.encode = function(data) {
  return this.opts.encode(data);
};

Queue.prototype.decode = function(data) {
  return this.opts.decode(data);
};

Queue.prototype.stop = function(cb) {
  var self = this;

  debug('stopping queue');

  async.each(self.workers, function(w, done) {
    w.stop(done);
  }, cb);
};
