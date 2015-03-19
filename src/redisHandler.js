var redis = require('redis');

module.exports = function() {
  var exp = {};
  exp.createClient = function() {
    return redis.createClient();
  };
  exp.client = function() {
    return exp._client || (exp._client = exp.createClient());
  };
  exp.pubsub = function() {
    return exp._pubsub || (exp._pubsub = exp.createClient());
  };
  return exp;
};
