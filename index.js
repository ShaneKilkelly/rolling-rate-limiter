var assert = require("assert");
var microtime = require("microtime-nodejs");

function RateLimiter (options) {
  var redis           = options.redis,
      interval        = options.interval * 1000, // in microseconds
      maxInInterval   = options.maxInInterval,
      minDifference   = options.minDifference ? 1000 * options.minDifference : null, // also in microseconds
      namespace       = options.namespace || (options.redis && ("rate-limiter-" + Math.random().toString(36).slice(2))) || null;

  assert(interval > 0, "Must pass a positive integer for `options.interval`");
  assert(maxInInterval > 0, "Must pass a positive integer for `options.maxInInterval`");
  assert(!(minDifference < 0), "`options.minDifference` cannot be negative");

  if (!options.redis) {
    var storage = {};
    var timeouts = {};
  }

  if (redis) {
    // If redis is going to be potentially returning buffers OR an array from
    // ZRANGE, need a way to safely convert either of these types to an array
    // of numbers.  Otherwise, we can just assume that the result is an array
    // and safely map over it.
    var zrangeToUserSet;
    if (redis.options.return_buffers || redis.options.detect_buffers) {
      zrangeToUserSet = function(str) {
        return String(str).split(",").map(Number);
      };
    } else {
      zrangeToUserSet = function(arr) {
        return arr.map(Number);
      };
    }

		var isProbablyIoRedisResult = function(r) {
			var probably = true;
			for (var i = 0; i < r.length; i++) {
        if (!(r[i] instanceof Object && r[i].hasOwnProperty('length') && r[i].length == 2)) {
					probably = false;
				}
			}
			return probably;
		};

		var getIoRedisResultErrors = function(r) {
			var errors = [];
			for (var i = 0; i < r.length; i++) {
				var currentItem = r[i];
				if (currentItem[0] !== null) {
					errors.push(currentItem[0]);
				}
			}
			return errors;
		};

		var flattenIoRedisResults = function(r) {
			var results = [];
			for (var i = 0; i < r.length; i++) {
				var currentItem = r[i];
				results.push(currentItem[1]);
			}
			return results;
		};

		var handleIoRedisResult = function(result) {
			var err = null;
			var finalResult = null;
			var isProbablyIoRedisResult = true;
			for (var i = 0; i < result.length; i++) {
        if (!(result[i] instanceof Object && result[i].hasOwnProperty('length'))) {
					isProbablyIoRedisResult = false;
				}
			}
			if (isProbablyIoRedisResult) {
				finalResult = [];
				for (var j = 0; j < result.length; j++) {
          var currentItem = result[j];
					if (currentItem[0] !== null) {
						err = currentItem[0];
					}
					finalResult.push(currentItem[1]);
				}
			} else {
        finalResult = result;
			}
			return [err, finalResult];
		};
    
    return function (id, cb) {
      if (!cb) {
        cb = id;
        id = "";
      }
      

      assert.equal(typeof cb, "function", "Callback must be a function.");
      
      var now = microtime.now();
      var key = namespace + id;
      var clearBefore = now - interval;

      var batch = redis.multi();
      batch.zremrangebyscore(key, 0, clearBefore);
      batch.zrange(key, 0, -1);
      batch.zadd(key, now, now);
      batch.expire(key, Math.ceil(interval / 1000000)); // convert to seconds, as used by redis ttl.
      batch.exec(function (err, resultArr) {
        if (err) return cb(err);

				if (isProbablyIoRedisResult(resultArr)) {
					var errors = getIoRedisResultErrors(resultArr);
					if (errors.length > 0) {
						// Just report the first error
            return cb(errors[0]);
					}
					resultArr = flattenIoRedisResults(resultArr);
				}

        var userSet = zrangeToUserSet(resultArr[1]);

        var tooManyInInterval = userSet.length >= maxInInterval;
        var timeSinceLastRequest = minDifference && (now - userSet[userSet.length - 1]);

        var result, remaining;
        if (tooManyInInterval || timeSinceLastRequest < minDifference) {
          result = Math.min(userSet[0] - now + interval, minDifference ? minDifference - timeSinceLastRequest : Infinity);
          result = Math.floor(result / 1000); // convert to miliseconds for user readability.
          remaining = -1;
        } else {
          remaining = maxInInterval - userSet.length - 1;
          result = 0;
        }

        return cb(null, result, remaining);
      });
    };
  } else {
    return function () {
      var args = Array.prototype.slice.call(arguments);
      var cb = args.pop();
      var id;
      if (typeof cb === "function") {
        id = args[0] || "";
      } else {
        id = cb || "";
        cb = null;
      }
      
      var now = microtime.now();
      var clearBefore = now - interval;

      clearTimeout(timeouts[id]);
      var userSet = storage[id] = (storage[id] || []).filter(function(timestamp) {
        return timestamp > clearBefore;
      });

      var tooManyInInterval = userSet.length >= maxInInterval;
      var timeSinceLastRequest = minDifference && (now - userSet[userSet.length - 1]);

      var result, remaining;
      if (tooManyInInterval || timeSinceLastRequest < minDifference) {
        result = Math.min(userSet[0] - now + interval, minDifference ? minDifference - timeSinceLastRequest : Infinity);
        result = Math.floor(result / 1000); // convert from microseconds for user readability.
        remaining = -1;
      } else {
        remaining = maxInInterval - userSet.length - 1;
        result = 0;
      }
      userSet.push(now);
      timeouts[id] = setTimeout(function() {
        delete storage[id];
      }, interval / 1000); // convert to miliseconds for javascript timeout

      if (cb) {
        return process.nextTick(function() {
          cb(null, result, remaining);
        });
      } else {
        return result;
      }
    };
  }
}

module.exports = RateLimiter;



