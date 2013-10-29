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
  this.ttl();
};

/**
 * Remove jobs from state queues based on TTL settings
 */
Queue.prototype.ttl = function() {
  var self = this;

  var runFrequency = 1000;
  clearTimeout(self.ttlTimeout);

  var scheduleNext = function(){
    self.ttlTimeout = setTimeout(self.ttl.bind(self), runFrequency);
  };

  var getStateOption = function(state){
    return self.getOption(state+'TTL');
  };

  var ttlSet = false;
  for (var i = 0; i < this.states.length; i++) {
    var state = this.states[i];
    ttlSet = (typeof getStateOption(state) == 'number');
  }

  if (!ttlSet) return scheduleNext();

  var removeJobs = function(jobs, cb){
    var batch = new Batch();

    for (var i = 0; i < jobs.length; i++) {
      var job = jobs[i];
      batch.push(job.remove.bind(job));
    }

    batch.end(cb);
  };

  self.setLock(self.redis, Math.ceil(runFrequency / 1000), 'qp:' + self.name, 'ttl', function(){
    var batch = new Batch();

    self.states.forEach(function(state){
      var ttl = getStateOption(state);

      if (typeof ttl != 'number') return;

      batch.push(function(done){
        self.jobsByState(state, ttl, function(err, jobs){
          if (err || !jobs) return;

          removeJobs(jobs, done);
        });
      });
    });

    batch.end(scheduleNext);
  });
};

/**
 * Get array of jobs by their state
 * @param  {string}   state
 * @param  {integer}   ttl  in ms
 * @param  {Function} cb
 */
Queue.prototype.jobsByState = function(state, ttl, cb) {
  var self = this;

  if (!ttl) cb = ttl;
  if (!cb) cb = function(){};

  var key = 'qp:' + self.name + '.' + state;
  if (ttl) end = Date.now() - ttl;
  else end = -1;

  self.redis.zrangebyscore(key, 0, end, function(err, members){
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

/**
 * Set a lock key
 * @param {object}   redis   node-redis client
 * @param {integer}  ttl     Time in seconds for the lock to live
 * @param {string}   suffix  Add uniqueness to the lock
 * @param {Function} cb
 */
Queue.prototype.setLock = function(redis, ttl, keyPrefix, suffix, cb) {
  var timestamp = ~~(new Date()/1000);
  timestamp -= timestamp % ttl;
  var checksum = require('crypto').createHash('md5').update(suffix + timestamp).digest('hex').substr(0, 10);
  var key = keyPrefix + 'lock:' + checksum;
  var m = redis.multi();
  m.setnx(key, timestamp, function(err, lockSet){
    if(err) return cb(err);

    return cb(err, +lockSet);
  });
  m.expire(key, ttl);
  m.exec();
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
    failedTTL: false
  };

  // use queue opts first
  if (this.opts.hasOwnProperty(key)) return this.opts[key];

  // try QP opts
  if (this.qp.opts.hasOwnProperty(key)) return this.qp.opts[key];

  // return the default (or undefined)
  return defaults[key];
};

Queue.prototype.getJobs = function(state, from, to, cb) {
  var self = this;

  self.redis.zrange('qp:' + this.name + '.' + state, from, to, function(err, jobs) {

    var batch = new Batch();
    jobs.forEach(function(id) {

      batch.push(function(done) {
        var job = self.create();
        job.id = id;
        job.getInfo(function() {
          job.toJSON(true);
          done(null, job);
        });
      });


    });
    batch.end(cb);
  });
};

Queue.prototype.flush = function(cb) {
  var r = this.redis.multi();

  debug('flushing');

  r.srem('qp:job:types', this.name);
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
  self.redis.zrange('qp:' + self.name + '.' + type, 0, -1, function(e, members){
    for (var i = 0; i < members.length; i++) {
      var job = self.create();
      job.id = members[i];
      job.state = type;
      job._remove(r);
    }
    r.exec(cb);
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
