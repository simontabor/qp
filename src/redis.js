var redis = require('redis');

exports.createClient = function(){
  return redis.createClient();
};

exports.client = function() {
  return exports._client || (exports._client = exports.createClient());
};
