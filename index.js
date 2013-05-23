var Batch = require('batch');

var Redis = require('./src/redis');

var Queue = require('./src/queue');
var Job = require('./src/job');


var QP = module.exports = function() {
  this.queues = {};
  this.redis = new Redis();
};

QP.prototype.getQueue = function(name) {
  return this.queues[name] || (this.queues[name] = new Queue(name));
};


QP.prototype.process = function(name, concurrency) {
  var callbacks = arguments.splice(2);

  // can pass an array instead of in arguments
  if (Array.isArray(callbacks[0])) callbacks = callbacks[0];

  var index = -1;
  var doNext = function() {
    callbacks[++index]();
  };
};
