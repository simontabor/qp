qp
==

Efficient queue manager/processor in node.js

## Install

```bash
$ npm install --save qp
```

## Usage

```javascript
var QP = require('qp');

var qp = new QP();

var q = qp.getQueue('my-queue');

var job = dr.createJob({
  qp: 'it works'
});

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
dr.process(function(job, done) {
  setTimeout(done, 1000);
});
```
