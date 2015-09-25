'use strict';
var Queue = require('./Queue');
var async = require('neo-async');
var Disque = require('disque-client');

var QP = module.exports = function(opts) {
  this.queues = {};
  this.opts = opts || {};

  this.disque = new Disque(opts.disque);
};

QP.prototype.getQueue = function(name, opts) {
  var q = this.queues[name] || (this.queues[name] = new Queue(this, name, opts || {}));
  q.setOpts(opts);
  return q;
};

QP.prototype.stop = function(cb) {
  var self = this;

  async.each(self.queues, function(q, done) {
    q.stop(done);
  }, function() {
    self.disque.quit(cb);
  });
};

QP.prototype.cleanShutdown = function(timeout, cb) {
  var self = this;

  if (!cb && typeof timeout === 'function') {
    cb = timeout;
    timeout = false;
  }

  var alreadyAttempted = false;
  var calledBack = false;
  var shutdownCB = cb || process.exit;

  [ 'SIGHUP', 'SIGINT', 'SIGTERM' ].forEach(function(sig) {
    process.on(sig, function() {
      // we've already received a shutdown command, exit immediately
      if (alreadyAttempted) {
        console.log('qp already attempted shutdown, forcing exit');
        calledBack = true;
        return shutdownCB();
      }

      console.log('qp caught signal ' + sig);
      alreadyAttempted = true;

      var shutdownTimeout;

      var cb = function() {
        if (calledBack) return;
        calledBack = true;
        clearTimeout(shutdownTimeout);
        shutdownCB();
      };

      if (timeout) {
        shutdownTimeout = setTimeout(cb, timeout);
      }

      self.stop(cb);
    });
  });
};
