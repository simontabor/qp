var R = require('redis-clustr');


var QP = require('../index');

var qp = new QP({
  cleanShutdown: true,
  noBlock: true,
  deleteOnFinish: true
});

qp.redisClient(function() {
  var r = new R({
    servers: [
      {
        port: 7006,
        host: '127.0.0.1'
      }
    ]
  });

  r.on('error', function(err) {
    console.trace(err);
  });

  return r;
});

var q = qp.getQueue('test');

var rate = 0;
setInterval(function() {
  console.log(rate);
  rate = 0;
}, 1000);

var jobs = [];
for (var i = 0; i < 200; i++) {
  jobs.push({ property: i });
}
q.multiSave(jobs, function() {
  q.process({
    concurrency: 1,
    maxRate: 20,
    minRate: 8,
    rateInterval: 1000
  }, function(job, done) {
    job.log('test log');
    rate++;
    setTimeout(done, 1000);
  });
});

