'use strict';
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var debug = require('debug')('qp:Job');

var Job = module.exports = function(queue, data, opts) {
  var self = this;

  self.data = data;
  self.queue = queue;
  self.disque = queue.disque;

  self.opts = {};
  self.setOpts(self.queue.opts);
  if (opts) self.setOpts(opts);
};

util.inherits(Job, EventEmitter);

Job.prototype.setOpts = function(opts) {
  var self = this;
  for (var i in opts) self.opts[i] = opts[i];
};

Job.prototype.getQueueCmd = function() {
  var self = this;

  var cmd = [];

  cmd.push(self.opts.timeout || 0);

  var replicate = self.opts.replicate;
  if (replicate) cmd.push('REPLICATE', replicate);

  var delay = self.opts.delay;
  if (delay) cmd.push('DELAY', delay);

  var retry = self.opts.retry;
  if (retry) cmd.push('RETRY', retry);

  var ttl = self.opts.ttl;
  if (ttl) cmd.push('TTL', ttl);

  var maxLen = self.opts.maxLen;
  if (maxLen !== Infinity) cmd.push('MAXLEN', maxLen);

  if (self.opts.async) cmd.push('ASYNC');

  return cmd;
};

Job.prototype.save = function(cb) {
  var self = this;

  var cmd = [ self.queue.name, self.queue.encode(self.data) ].concat(self.getQueueCmd());

  self.disque.addjob(cmd, function(err, id) {
    if (err) return cb(err);
    self.id = id;
    cb(err);
  });
};

Job.prototype.remove = function(cb) {
  var self = this;
  self.disque.dequeue(self.id, cb);
};

Job.prototype.error = function(err) {
  var self = this;

  debug('failed job');

  self.emit('error', err);
};

Job.prototype.done = function(err) {
  var self = this;

  if (self.finished) {
    debug('already finished');
    return;
  }

  if (err) {
    self.error(err);
    return;
  }

  debug('completed job');

  self.emit('done');
};
