var util = require('util');
var EventEmitter = require('events').EventEmitter;
var debug = require('debug')('qp:Job');

var f = function(e) {
  if (e) console.log(e);
};

var Job = module.exports = function(queue, data, opts) {
  this.data = data;
  this.queue = queue;
  this.state = 'unsaved';
  this.redis = queue.redis;

  // doesnt currently do anything, the job uses the queue options
  this.opts = opts || {};
};

util.inherits(Job, EventEmitter);

Job.prototype.getID = function(cb) {
  var self = this;

  debug('getting id');

  // this allows the user to set an ID themselves
  if (self.id) {
    setImmediate(function() {
      cb(null, self.id);
    });
    return;
  }

  if (self.queue.getOption('randomID')) {
    self.id = Math.random().toString().slice(2);
    setImmediate(function() {
      cb(null, self.id);
    });
    return;
  }

  // if no id is set, increment a counter and use that (redis round trip, not recommended)
  self.redis.incr('qp:' + self.queue.name + '.ids', function(err, id) {
    self.id = id;
    cb(err, id);
  });
};

Job.prototype.save = function(cb) {
  var self = this;

  var r = self.redis.multi();

  self._save(r, function(err) {
    if (err) return cb(err);

    self._exec(r, cb);
  });

  return this;
};


Job.prototype._save = function(r, cb) {
  var self = this;

  debug('saving');


  var save = function() {
    self._saved = true;

    var props = {
      type: self.queue.name,
      created_at: Date.now()
    };

    if (self.data) props.data = self.data;
    if (self._timeout) props._timeout = self._timeout;
    if (self._attempts) props._attempts = self._attempts;

    self.multiSet(props, r);

    self.enqueue(r);

    cb();
  };

  self.getID(function(err, id) {

    if (self.queue.getOption('unique')) {
      debug('checking uniqueness');

      self.redis.sadd('qp:' + self.queue.name + ':unique', id, function(err, res) {
        // check if added
        if (!res) {
          debug('already queued');
          return cb(err, res);
        }

        save();
      });

      return;
    }

    save();

  });
};

Job.prototype.enqueue = function(r) {

  debug('enqueueing');

  if (this._attempts) r.hincrby('qp:job:' + this.queue.name + '.' + this.id, '_attempted', 1);

  var command = 'rpush';

  // allow jobs to be added to the top of the queue
  // may cause incorrect ordering if zsets are enabled
  if (this.opts.top) command = 'lpush';
  r[command]('qp:' + this.queue.name + ':jobs', this.id);

  this.setState('inactive', r);

  return this;
};


Job.prototype.set = function(key, val, r, cb) {
  this[key] = val;

  if (!this._saved || this.queue.getOption('noInfo') || (this.queue.getOption('noMeta') && key !== 'data' && key[0] !== '_')) {
    if (cb) cb();
    return this;
  }

  if (typeof val === 'object') val = JSON.stringify(val || {});

  (r || this.redis).hset('qp:job:' + this.queue.name + '.' + this.id, key, val, cb || f);

  return this;
};

Job.prototype.multiSet = function(props, r, cb) {
  for (var i in props) {
    this[i] = props[i];
  }

  if (!this._saved || this.queue.getOption('noInfo')) {
    if (cb) cb();
    return this;
  }

  var noMeta = this.queue.getOption('noMeta');
  var hasProps = false;
  var redisProps = {};
  for (var i in props) {
    if (noMeta && i !== 'data' && i[0] !== '_') continue;
    hasProps = true;
    if (props[i] && typeof props[i] === 'object') {
      redisProps[i] = JSON.stringify(props[i]);
    } else {
      redisProps[i] = props[i];
    }
  }

  if (!hasProps) {
    if (cb) return cb();
    return this;
  }

  (r || this.redis).hmset('qp:job:' + this.queue.name + '.' + this.id, redisProps, cb || f);

  return this;
};

Job.prototype.setState = function(state, r) {
  if (!r) r = this.redis;

  debug('setting state');

  var zsets = this.queue.getOption('zsets');

  if (zsets && this.state && this.state !== 'unsaved') r.zrem('qp:' + this.queue.name + '.' + this.state, this.id, f);

  this.state = state;
  this.set('state', state, r);

  if (zsets) r.zadd('qp:' + this.queue.name + '.' + state, Date.now(), this.id, f);

  this._emit('state', state, r);

  return this;

};

Job.prototype.getInfo = function(cb) {
  var self = this;

  if (!this.id) return cb(null);

  debug('getting info');

  this.redis.hgetall('qp:job:' + this.queue.name + '.' + this.id, function(err, data) {
    if (err || !data) {
      return cb(err);
    }

    self._processInfo(data);
    cb();
  });

};

Job.prototype._processInfo = function(info) {
  if (!info || typeof info !== 'object') return false;

  this.data = {};

  if (info.data) {
    try {
      this.data = JSON.parse(info.data);
    } catch(e) {
      console.log('QP: error parsing json', info.data);
      console.error(e);
    }
  }

  this.state = info.state;
  this.type = info.type;
  if (info.created_at) this.created_at = new Date(+info.created_at);
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

  this._emit('progress', {
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
  if (!this.queue.getOption('noLogs')) {
    this.redis.rpush('qp:job:' + this.queue.name + ':log.' + this.id, msg);
  }
  this._emit('log', msg);

  return this;
};

Job.prototype.getLog = function(cb) {
  if (this.queue.getOption('noLogs')) return cb(null, []);
  this.redis.lrange('qp:job:' + this.queue.name + ':log.' + this.id, 0, -1, cb);
};


Job.prototype._emit = function(type, msg, r) {
  if (!r) r = this.redis;

  var data = {
    type: type,
    queue: this.queue.name,
    job: this.toJSON(),
    message: msg
  };

  this.emit(type, data);

  if (this.queue.getOption('pubSub')) r.publish('qp:events', JSON.stringify(data));
};


Job.prototype.remove = function(cb) {
  var r = this.redis.multi();
  this._remove(r);

  self._exec(r, cb);
};

Job.prototype._remove = function(r) {

  if (this.queue.getOption('zsets')) r.zrem('qp:' + this.queue.name + '.' + this.state, this.id);

  var delKeys = [];
  if (!this.queue.getOption('noInfo')) delKeys.push('qp:job:' + this.queue.name + '.' + this.id);
  if (!this.queue.getOption('noLogs')) delKeys.push('qp:job:' + this.queue.name + ':log.' + this.id);
  if (delKeys.length) r.del(delKeys);

  if (this.queue.getOption('unique')) r.srem('qp:' + this.queue.name + ':unique', this.id);
};

Job.prototype._finish = function(r) {
  var self = this;

  self.finished = true;

  debug('finishing job');

  // this is handy for fast queues and for unique queues (avoids data conflicts if id's are reused)
  if (self.queue.getOption('deleteOnFinish')) {
    self._remove(r);
  } else if (self.queue.getOption('unique')) {
    r.srem('qp:' + self.queue.name + ':unique', self.id);
  }

  self._exec(r, function() {
    self._emit('done');
  });
};

Job.prototype.error = function(err) {
  var self = this;

  self.log('attempt failed:' + err);

  if (self._attempts && (self._attempts > self._attempted || !self._attempted)) {
    var r = self.redis.multi();
    debug('requeueing');
    self.enqueue(r);
    self.finished = true;
    self._exec(r, function() {
      self._emit('done');
    });
    return;
  }

  this.fail(err);
};

Job.prototype.fail = function(err) {
  debug('failed job');

  var r = this.redis.multi();

  this.setState('failed', r);
  this.multiSet({
    failed_at: Date.now(),
    error_message: err
  }, r);

  this._finish(r);
};

Job.prototype.done = function(err) {
  var self = this;

  if (self.finished) {
    debug('already finished');
    return;
  }

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

// efficiently execute a multi (works across old and new node_redis versions)
Job.prototype._exec = function(r, cb) {
  if (!cb) cb = function() {};

  if (r.exec_atomic) {
    return r.exec_atomic(cb);
  }

  var firstMulti = ((r.queue[0] || [])[0] || '').toLowerCase() === 'multi';
  if (!r.queue.length || (firstMulti && r.queue.length === 1)) {
    return setImmediate(cb);
  }

  if (!firstMulti || r.queue.length > 2) {
    return r.exec(cb);
  }


  var args = r.queue[1];
  var cmd = args.shift();
  var fnCb;
  if (typeof args[args.length - 1] === 'function') fnCb = args.pop();

  this.redis[cmd](args, function(err, resp) {
    if (fnCb) fnCb(err, resp);
    cb(err, [ resp ]);
  });
};

