var Batch = require('batch');

var redis = require('./redis');
var Job = require('./job');
var Workers = require('./workers');

var debug = require('debug')('qp:Queue');

var Queue = module.exports = function(qp, name, opts) {
  this.name = name;
  this.redis = redis.client();
  this.qp = qp;
  this.workers = [];
  this.opts = opts;
  this.states = ['active', 'inactive', 'completed', 'failed'];

  // add the queue name to the jobtypes set
  this.redis.sadd('qp:job:types', this.name);

  if (this.getOption('ttlRunFrequency')) this.ttl();
};


Queue.prototype.getOption = function(key) {
  var defaults = {
    noInfo: false,
    pubSub: true,
    noBlock: false,
    checkInterval: 200,
    unique: false,
    deleteOnFinish: false,
    zsets: true,
    inactiveTTL: false,
    activeTTL: false,
    completedTTL: false,
    failedTTL: false,
    ttlRunFrequency: false,
    maxSpawn: Infinity
  };

  // use queue opts first
  if (this.opts.hasOwnProperty(key)) return this.opts[key];

  // try QP opts
  if (this.qp.opts.hasOwnProperty(key)) return this.qp.opts[key];

  // return the default (or undefined)
  return defaults[key];
};


Queue.prototype.create = Queue.prototype.createJob = function(data, opts) {
  var job = new Job(this, data, opts);
  return job;
};


// helper to set the job id as well as any data
Queue.prototype.job = function(id, data, opts) {
  var job = this.create(data, opts);
  job.id = id;
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

  // stop redis storming if uniqueness is enabled or IDs arent set
  batch.concurrency(3);

  jobs.forEach(function(job) {
    batch.push(function(done) {
      job._save(r, done);
    });
  });

  batch.end(function() {
    // how is best to handle errors from _save here?
    // we probably still want to exec the rest otherwise
    // one bad job stops the entire batch from being saved
    r.exec(cb);
  });

};


Queue.prototype.process = function(opts, cb) {

  // allow concurrency not to be set
  if (typeof opts == 'function' && !cb) {
    cb = opts;
    opts = {};
  }

  if (typeof opts === 'number') {
    opts = { concurrency: opts };
  }

  var workers = new Workers(this, this.redis);
  this.workers.push(workers);

  workers.process(opts, cb);

};


Queue.prototype.numJobs = function(states, cb) {
  var self = this;

  if (typeof states === 'function' && !cb) {
    cb = states;

    // default to all states, including the queued list
    states = self.states.concat('queued');
  }

  if (!Array.isArray(states)) states = [states];

  var data = {};

  debug('getting number of jobs');

  var batch = new Batch();

  states.forEach(function(state) {
    batch.push(function(done) {
      if (state === 'queued') {
        self.redis.llen('qp:' + self.name + ':jobs', function(e, r) {
          data[state] = r;
          done();
        });
      } else {
        self.redis.zcard('qp:' + self.name + '.' + state, function(e, r) {
          data[state] = r;
          done();
        });
      }
    });
  });

  batch.end(function() {
    cb(null, data);
  });

};


Queue.prototype.getJobs = function(state, from, to, cb) {
  var self = this;

  self.redis.zrange('qp:' + this.name + '.' + state, from, to, function(err, members) {
    if (err) return cb(err);

    var jobs = [];
    for (var i = 0; i < members.length; i++) {
      var job = self.create();
      job.id = members[i];
      job.state = state;
      jobs.push(job);
    }

    cb(err, jobs);
  });
};


// gets jobs by the time they were inserted into that state
Queue.prototype.getJobsByTime = function(state, from, to, cb) {
  var self = this;

  if (typeof to === 'function' && !cb) {
    // [state, to, cb] argument format
    cb = to;
    to = from;
    from = 0;
  }

  var key = 'qp:' + self.name + '.' + state;

  self.redis.zrangebyscore(key, from, to, function(err, members){
    if (err) return cb(err);

    var jobs = [];
    for (var i = 0; i < members.length; i++) {
      var job = self.create();
      job.id = members[i];
      job.state = state;
      jobs.push(job);
    }

    cb(err, jobs);
  });
};


Queue.prototype.setTTLLock = function(cb) {
  var self = this;

  var ttl = self.getOption('ttlRunFrequency');

  var timestamp = Date.now();

  // round the timestamp to the last run
  timestamp -= timestamp % ttl;

  var key = 'qp:' + self.name + ':lock.' + timestamp;

  var m = self.redis.multi();
  m.setnx(key, timestamp, function(err, lockAcquired){
    cb(err, +lockAcquired);
  });
  m.pexpire(key, ttl);
  m.exec();
};



Queue.prototype.ttl = function() {
  var self = this;

  clearTimeout(self.ttlTimeout);

  var scheduleNext = function(){
    self.ttlTimeout = setTimeout(self.ttl.bind(self), self.getOption('ttlRunFrequency'));
  };

  self.setTTLLock(function(err, lockAcquired){
    if (err){
      debug('unable to set ttl check lock');
      return scheduleNext();
    }

    if (!lockAcquired) return scheduleNext();

    var batch = new Batch();
    batch.concurrency(1);

    self.states.forEach(function(state){
      var ttl = self.getOption(state+'TTL');

      // not set or invalid
      if (typeof ttl != 'number') return;

      batch.push(function(done){
        self.getJobsByTime(state, 0, Date.now() - ttl, function(err, jobs){
          if (err || !jobs || !jobs.length) return done();

          debug('removing %d jobs in %s state past ttl of %d', jobs.length, state, ttl);
          self.removeJobs(jobs, done);
        });
      });
    });

    batch.end(function(err){
      if (err) debug(err);

      scheduleNext();
    });
  });
};


Queue.prototype.removeJobs = function(jobs, cb) {
  var self = this;

  var batch = new Batch();

  for (var i = 0; i < jobs.length; i++) {
    var job = jobs[i];

    // allow an array of job IDs
    if (!(job instanceof Job)) {
      job = self.job(job);
    }

    batch.push(job.remove.bind(job));
  }

  batch.end(cb);
};


Queue.prototype.clear = function(type, cb) {
  var self = this;

  debug('clearing');

  if (!cb && typeof type === 'function') {
    cb = type;
    type = 'completed';
  }

  if (!type) type = 'completed';

  var r = self.redis.multi();
  self.redis.zrange('qp:' + self.name + '.' + type, 0, -1, function(err, members) {
    self.removeJobs(members, cb);
  });
};


Queue.prototype.stop = function(cb) {
  debug('stopping queue');

  var batch = new Batch();
  for (var i = 0; i < this.workers.length; i++) {
    var w = this.workers[i];
    batch.push(w.stop.bind(w));
  }
  batch.end(cb);
};
