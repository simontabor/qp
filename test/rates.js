var QP = require('../index');
var qp = new QP({
  disque: {
    servers: [
      {
        host: '127.0.0.1',
        port: 7711
      }
    ]
  }
});

var q = qp.getQueue('test', {
  count: 10,
  fastAck: true,
  ackDelay: 1000,
  // getTimeout: 0,
  async: true,
  retry: 5,
  nacks: 99
});

var job = q.create({ wat: true });

setInterval(function() {
  job.save(function(err) {
  //   // console.log(err, job);
  });
}, 1);





var w = q.rateProcess({
  jobs: 100,
  interval: 1000
}, function(job, done) {
  // console.log(job.id, job.counters);
  // done('err');
  done();
});

setInterval(function() {
  // console.log(w.jobsComplete);
  w.setOpts({ jobs: w.opts.jobs  + 50 });
}, 1000);
