# qp

[![Dependencies](https://david-dm.org/simontabor/qp.svg)](https://david-dm.org/simontabor/qp)
[![Join the chat at https://gitter.im/simontabor/qp](https://img.shields.io/badge/gitter-join%20chat-blue.svg)](https://gitter.im/simontabor/qp)

[![NPM](https://nodei.co/npm/qp.png?downloads=true&downloadRank=true&stars=true)](https://www.npmjs.com/package/qp)


Efficient queue manager/processor in node.js. Uses [disque](https://github.com/antirez/disque)

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

// save the job for processing
job.save();

// or just default to one at a time
q.process(function(job, done) {
  setTimeout(done, 1000);
});

// stop processing cleanly
q.stop(function() {
  console.log('stopped');
});

```

---

TODO: Document all options and advanced usage.
