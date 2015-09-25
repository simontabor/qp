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
  count: 100,
  fastAck: true,
  ackDelay: 0,
  getTimeout: 50,
  async: true,
  retry: 5,
  nacks: 99
});

var job = q.create({ wat: true });

setInterval(function() {
  job.save(function(err) {
  //   // console.log(err, job);
  });
}, 1000);





q.process(function(job, done) {
  console.log(job.id, job.counters);
  // done('err');
  done();
});
