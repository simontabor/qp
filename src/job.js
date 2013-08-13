var Batch = require('batch');

var redis = require('./redis');
var debug = require('debug')('qp:Job');

var f = function(e,r){
  if (e) console.log(e);
};

var Job = module.exports = function(data, queue) {
  this.data = data;
  this.queue = queue;
  this.state = 'unsaved';
  this.redis = redis.client();
};

Job.prototype.getID = function(cb) {
  var self = this;

  debug('getting id');

  if (this.id) return cb(this.id);

  this.redis.incr('qp:' + this.queue.name + '.ids', function(err, id) {
    self.id = id;
    cb(id);
  });
};

Job.prototype.save = function(cb) {
  var self = this;

  var r = self.redis.multi();

  self._save(r, function() {
    r.exec(cb);
  });

  return this;
};


Job.prototype._save = function(r, cb) {
  var self = this;

  r.sadd('qp:job:types', self.queue.name);

  debug('saving');

  self._saved = true;

  self.getID(function() {

    self
      .set('type', self.queue.name, r)
      .set('created_at', Date.now(), r)
      .set('data', self.data, r);

    if (self._timeout) {
      self.set('_timeout', self._timeout, r);
    }

    if (self._attempts) {
      self.set('_attempts', self._attempts, r);
    }

    self.enqueue(r);

    cb();
  });
};

Job.prototype.enqueue = function(r) {

  debug('enqueueing');

  if (this._attempts) r.hincrby('qp:job:' + this.queue.name + '.' + this.id, '_attempted', 1);
  r.rpush('qp:' + this.queue.name +':jobs', this.id);

  this.setState('inactive', r);

  return this;
};


Job.prototype.set = function(key, val, r, cb){
  this[key] = val;

  if (!this._saved) {
    return this;
  }

  if (typeof val === 'object') val = JSON.stringify(val || {});

  (r || this.redis).hset('qp:job:' + this.queue.name + '.' + this.id, key, val, cb || f);

  return this;
};

Job.prototype.setState = function(state, r, cb) {
  if (!r) r = this.redis;

  debug('setting state');

  r.zrem('qp:' + this.queue.name + '.' + this.state, this.id, f);

  this.state = state;

  this.set('state', state, r);

  r.zadd('qp:' + this.queue.name + '.' + state, this.id, this.id, f);

  this.emit('state', state, r);

  return this;

};

Job.prototype.getInfo = function(cb) {
  var self = this;

  if (!this.id) return cb(null);

  debug('getting info');

  this.redis.hgetall('qp:job:' + this.queue.name + '.' + this.id, function(e, data) {
    self._processInfo(data);
    cb();
  });

};

Job.prototype._processInfo = function(info) {
  try {
    this.data = JSON.parse(info.data || {});
  } catch(e) {
    console.log('error parsing json');
    console.error(e);
    this.data = {};
  }
  this.state = info.state;
  this.type = info.type;
  this.created_at = new Date(+info.created_at);
  this._progress = info._progress;
  this._attempts = info._attempts && !isNaN(+info._attempts) && +info._attempts;
  this._attempted = +(info._attempted || 0);
  this._timeout = info._timeout && !isNaN(+info._timeout) && +info._timeout;
  this.error_message = info.error_message;

  if (info.completed_at) this.completed_at = new Date(+info.completed_at);
  if (info.updated_at) this.updated_at = new Date(+info.updated_at);

};

Job.prototype.toJSON = function(set) {
  var json = {
    id: this.id,
    queue: this.queue.name,
    data: this.data,
    state: this.state,
    created_at: this.created_at,
    progress: this._progress,
    completed_at: this.completed_at,
    attempts: this._attempts,
    attempted: this._attempted,
    timeout: this._timeout,
    error_message: this.error_message
  };
  if (set) this.json = json;
  return json;
};

Job.prototype.progress = function(done, total) {
  var progress = ~~(done / total * 100);

  debug('setting progress');

  this.set('_progress', progress);

  this.emit('progress', {
    done: done,
    total: total,
    progress: progress
  });

  return this;
};

Job.prototype.attempts = function(num) {
  this.set('_attempts', num);

  return this;
};

Job.prototype.timeout = function(num) {
  this.set('_timeout', num);

  return this;
};

Job.prototype.log = function(msg) {
  this.redis.rpush('qp:job:' + this.queue.name + ':log.' + this.id, msg);
  this.emit('log', msg);

  return this;
};

Job.prototype.getLog = function(cb) {
  this.redis.lrange('qp:job:' + this.queue.name + ':log.' + this.id, 0, -1, cb);
};


Job.prototype.emit = function(type, msg, r) {
  if (!r) r = this.redis;

  var data = {
    type: type,
    queue: this.queue.name,
    job: this.toJSON(),
    message: msg
  };

  r.publish('qp:events', JSON.stringify(data));
};


Job.prototype.remove = function(cb) {
  var r = this.redis.multi();
  this._remove(r);
  r.exec(cb);
};

Job.prototype._remove = function(r) {
  r.zrem('qp:' + this.queue.name + '.' + this.state, this.id);
  r.del('qp:job:' + this.queue.name + '.' + this.id);
  r.del('qp:job:' + this.queue.name + ':log.' + this.id);
};

Job.prototype._finish = function(r) {
  var self = this;

  debug('finishing job');

  r.exec(function() {
    self.worker.process();
  });
};

Job.prototype.error = function(err) {
  this.log('attempt failed:' + err);

  if (this._attempts && (this._attempts > this._attempted || !this._attempted)) {
    var r = this.redis.multi();
    this.enqueue(r);
    this._finish(r);
    return;
  }

  this.fail(err);
};

Job.prototype.fail = function(err) {
  var self = this;

  debug('failed job');

  var r = this.redis.multi();

  this
    .set('failed_at', Date.now(), r)
    .setState('failed', r)
    .set('error_message', err);

  this._finish(r);

};

Job.prototype.done = function(err) {
  var self = this;

  debug('completed job');

  // clear any fail timeout if there is one
  clearTimeout(self.__timeout);
  if (err) {
    self.error(err);
    return;
  }

  var r = this.redis.multi();

  this
    .set('completed_at', Date.now(), r)
    .setState('completed', r);

  this._finish(r);
};

