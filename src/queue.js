var Batch = require('batch');

var redis = require('./redis');
var Job = require('./job');
var Worker = require('./worker');

var Queue = module.exports = function(name) {
  this.name = name;
  this.redis = redis.client();
};

Queue.prototype.create = Queue.prototype.createJob = function(data) {

  var job = new Job(data);
  job.queue = this;

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
      job.setState('active');
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

Queue.prototype.numJobs = function(states, cb) {
  var self = this;

  if (!Array.isArray(states)) states = [states];

  var data = {};

  var batch = new Batch();

  states.forEach(function(state) {
    batch.push(function(done) {
      self.redis.zcard('qp:' + self.name + '.' + state, function(e, r) {
        data[state] = r;
        done();
      });
    });
  });

  batch.end(function() {
    cb(null, data);
  });

};

Queue.prototype.flush = function(cb) {
  var r = this.redis.multi();

  r.srem('qp:job:types', this.name);
};

Queue.prototype.clear = function(cb) {
  var self = this;

  var r = self.redis.multi();
  self.redis.zrange('qp:' + self.name + '.completed', 0, -1, function(e, members){
    for (var i = 0; i < members.length; i++) {
      var job = self.create();
      job.id = members[i];
      job.state = 'completed';
      job._remove(r);
    }
    r.exec(cb);
  });
};
