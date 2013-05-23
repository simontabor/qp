var redis = require('redis');

var R = module.exports = function() {};

R.prototype.createClient = function(){
  return redis.createClient();
};

R.prototype.client = function() {
  return this._client || (this._client = this.createClient());
};
