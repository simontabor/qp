var Batch = require('batch');

var redis = require('./redis');

var f = function(e,r){
  if (e) console.log(e);
};

var Job = module.exports = function(data) {
  this.data = data;
  this.state = 'unsaved';
  this.redis = redis.client();
};

Job.prototype.getID = function(cb) {
  var self = this;

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

  self.getID(function() {

    self
      .set('type', self.queue.name, r)
      .set('created_at', Date.now(), r)
      .set('data', self.data, r)
      .setState('inactive', r);

    r.rpush('qp:' + self.queue.name +':jobs', self.id);

    cb();
  });
};


Job.prototype.set = function(key, val, r, cb){
  this[key] = val;

  if (typeof val === 'object') val = JSON.stringify(val || {});

  (r || this.redis).hset('qp:job:' + this.queue.name + '.' + this.id, key, val, cb || f);

  return this;
};

Job.prototype.setState = function(state, r, cb) {
  if (!r) r = this.redis;

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
  this._attempts = info._attempts;

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
    completed_at: this.completed_at
  };
  if (set) this.json = json;
  return json;
};

Job.prototype.progress = function(done, total) {
  var progress = ~~(done / total * 100);

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

Job.prototype.done = function(err, msg) {
  var self = this;

  if (err) {

  }

  var r = this.redis.multi();

  this
    .set('completed_at', Date.now(), r)
    .setState('completed', r);

  r.exec(function() {
    self.worker.process();
  });
};

