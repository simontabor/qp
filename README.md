# qp

Efficient queue manager/processor in node.js.

Has a server/admin interface, available at [qp-server](https://github.com/simontabor/qp-server).

## Install

```bash
$ npm install --save qp
```

## Basic Usage

```javascript
var QP = require('qp');

var qp = new QP();

var q = qp.getQueue('my-queue');

var job = q.create({
  any: 'data'
});

// if you want to save a redis round trip, assign the job an ID.
job.id = Math.random().toString().slice(2);

// save the job for processing
job.save();

var jobs = [
  {
    qp: 'does it really work?'
  },
  {
    any: 'data'
  },
  {
    cango: 'here'
  }
];

q.multiSave(jobs, function(e,r) {
  // all the above jobs have been saved
});

// process jobs, concurrency of 3
q.process(3, function(job, done) {
  setTimeout(done, 5000);
});

// or just default to one at a time
q.process(function(job, done) {
  setTimeout(done, 1000);
});

// stop processing cleanly
q.stop(function() {
  console.log('stopped');
});

```

## Options

```javascript
// QP
var qp = new QP({
  cleanShutdown: true, // default: false. set to true to have all workers complete all jobs prior to process exit
  shutdownCB: function() { process.exit() } // default: process.exit. set to you own function to handle exits cleanly
});

// any of these options can be specified above as options to new QP. Any options here will override those specified for QP.
var q = qp.getQueue('test', {
  noInfo: true, // default: false. set to true to disable arbitary job data (id only). reduces redis usage **BETA**
  pubSub: false, // default: true. set to false to disable redis pubsub (used for the server/UI) which will reduce redis load
  noBlock: true, // default: false. set to true to not use blpop on redis (reduces number of connections),
  checkInterval: 20 // default: 200. if noBlock is true, number of ms to wait if no job is returned before checking again
  unique: true, // default: false. whether or not each job ID should be checked that it's not already in the queue
  deleteOnFinish: true, // default: false. if true, jobs will be removed from redis as soon as they're completed. useful for high throughput queues where historical job records arent needed
  zsets: false // default: true. whether or not to maintain zsets of active, completed, failed and active jobs. will reduce redis ops if disabled but will remove numJobs + server functionality. will probably need deleteOnFinish to be true for this not to leave old jobs in redis
});

// processing
q.process({
  concurrency: 10, // default: 1. number of workers to spawn (if min/max rates are set then concurrency will be automatically adjusted)
  maxRate: 20, // default: Infinity (no limit). maximum number of jobs to process per `rateInterval`
  minRate: 8, // default: 0 (none). minimum number of jobs to process per `rateInterval`
  rateInterval: 2000, // default: 1000. number of ms to check the rates over
  checkInterval: 1000, // default: rateInterval / 10. number of ms to wait between checking the processing rate (lower = higher accuracy and less bursting)
  maxSpawn: 1 // default: Infinity. the maximum number of new workers to spawn when we're going too slowly
}, function(job, done) {
  done();
});

```
