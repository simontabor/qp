var Batch = require('batch');

var Redis = require('./redis');
var Job = require('./job');
var Worker = require('./worker');

var redis = new Redis();

var Queue = module.exports = function(name) {
  this.name = name;
  this.redis = redis.client();
};


Queue.prototype._removeJob = function(job, r) {
  if (!r) r = this.redis;

  r.zrem('qp:' + this.name + '.' + job.state, job.id);
};

Queue.prototype.create = Queue.prototype.createJob = function(data) {

  var job = new Job(data);
  job.queue = this;
  job.redis = this.redis;

  return job;

};

Queue.prototype.multiSave = function(jobs, cb) {

  var self = this;

  for (var i = 0; i < jobs.length; i++) {
    if (!(jobs[i] instanceof Job)) {
      jobs[i] = self.create(jobs[i]);
    }
  }

  var r = this.redis.multi();

  var batch = new Batch();
  batch.concurrency(3);

  jobs.forEach(function(job) {
    batch.push(function(done) {
      job._save(r, done);
    });
  });

  batch.end(function() {
    r.exec(cb);
  });

};

Queue.prototype._spawnWorker = function(cb) {
  var self = this;

  var w = new Worker(this);

  w.on('job', function(jobID) {
    var job = self.create();
    job.id = jobID;
    job.worker = w;

    job.getInfo(function() {
      cb(job, job.done.bind(job));
    });

  });

  w.process();
};

Queue.prototype.process = function(concurrency, cb) {

  // allow concurrency not to be set
  if (typeof concurrency == 'function' && !cb) {
    cb = concurrency;
    concurrency = null;
  }

  for (var i = 0; i < (concurrency || 1); i++) {
    this._spawnWorker(cb);
  }

};
