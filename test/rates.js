// basic test to see what happens at various processing rates

var QP = require('../index');

var qp = new QP({ noInfo: true, cleanShutdown: true });

var q = qp.getQueue('test');

var rate = 0;
setInterval(function() {
  console.log(rate);
  rate = 0;
}, 1000);


q.multiSave(new Array(200), function() {
  q.process({
    concurrency: 1,
    maxRate: 20,
    minRate: 8,
    rateInterval: 1000
  }, function(job, done) {
    setTimeout(done, 1000);
  });
});

