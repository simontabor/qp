var Queue = require('./src/queue');

var QP = module.exports = function() {
  this.queues = {};
};

QP.prototype.getQueue = function(name) {
  return this.queues[name] || (this.queues[name] = new Queue(name));
};
