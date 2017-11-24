"use strict";

var crypto = require("crypto"),
    path = require("path"),
    stream = require("stream"),
    url = require("url");

var _ = require("highland"),
    async = require("async"),
    level = require("level"),
    leveldown = require("leveldown"),
    lockingCache = require("locking-cache"),
    SphericalMercator = require("@mapbox/sphericalmercator"),
    sublevel = require("level-sublevel"),
    ts = require("tilelive-streaming");

var lockedOpenDb = lockingCache({
      max: 10
    }),
    mercator = new SphericalMercator();

var openDb = lockedOpenDb(function(path, lock) {
  var key = crypto.createHash("sha1").update(path).digest().toString("hex");

  // lock
  return lock(key, function(unlock) {
    return level(path, {
      valueEncoding: "binary"
    }, unlock);
  });
});

var open = function(path, options, callback) {
  var prefix = crypto.createHash("sha1").update(JSON.stringify(options)).digest().toString("hex");

  return async.waterfall([
    async.apply(openDb, path),
    function(db, done) {
      return done(null, sublevel(db).sublevel(prefix));
    }
  ], callback);
};

// TODO tell tilelive-cache to avoid caching
var LevelDB = function(uri, callback) {
  if (typeof(uri) === "string") {
    uri = url.parse(uri, true);
  }

  uri.query = uri.query || {};

  this.uri = uri;
  this.path = path.join(uri.hostname, uri.pathname);

  // support for multi-scale/multi-format archives
  this.format = uri.query.format || "";
  // id for archives containing multiple styles
  this.id = uri.query.id || "";
  this.scale = uri.query.scale || 1;

  var self = this;

  this.cargo = async.cargo(function(operations, next) {
    return async.waterfall([
      async.apply(self.open.bind(self)),
      function(db, done) {
        return db.batch(operations, done);
      }
    ], next);
  });

  return setImmediate(callback, null, this);
};

LevelDB.prototype.createReadStream = function(options) {
  var source = this,
      readable = new stream.PassThrough({
        objectMode: true
      });

  source.getInfo(function(err, info) {
    if (err) {
      return readable.emit("error", err);
    }

    return source.open(function(err, db) {
      if (err) {
        return readable.emit("error", err);
      }

      options = ts.restrict(ts.applyDefaults(options), info);
      info = ts.clone(info);

      // restrict info according to options and emit it
      readable.emit("info", ts.restrict(info, options));

      return _(function(push, next) {
        for (var zoom = options.minzoom; zoom <= options.maxzoom; zoom++) {
          push(null, zoom);
        }

        return push(null, _.nil);
      }).map(function(zoom) {
        var xyz = mercator.xyz(options.bounds, zoom),
            x = xyz.minX;

        return _(function(push, next) {
          if (x > xyz.maxX) {
            return push(null, _.nil);
          }

          push(null, [
            [zoom, x, xyz.minY],
            [zoom, x, xyz.maxY]
          ]);

          x++;

          return next();
        });
      }).sequence().map(function(range) {
        var readStream = new stream.PassThrough({
          objectMode: true
        });

        readStream._transform = function(obj, _, callback) {
          var key = obj.key,
              value = obj.value,
              coords = key.split(":", 2).pop(),
              parts = coords.split("/"),
              z = parts.shift() | 0,
              x = parts.shift() | 0,
              y = parts.shift() | 0,
              out = new ts.TileStream(z, x, y);

          return db.get("headers:" + coords, {
            valueEncoding: "json"
          }, function(err, headers) {
            if (err) {
              return callback(err);
            }

            readStream.push(out);

            out.setHeaders(headers);

            out.end(value);

            return callback();
          });
        };

        return _(db.createReadStream({
          gte: "data:" + range[0].join("/"),
          lte: "data:" + range[1].join("/"),
          valueEncoding: "binary"
        }).pipe(readStream));
      }).sequence().pipe(readable);
    });
  });

  return readable;
};

LevelDB.prototype.getTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  var key = [zoom, x, y].join("/");

  return async.waterfall([
    async.apply(this.open.bind(this)),
    function(db, done) {
      var get = db.get.bind(db);

      return async.parallel({
        headers: async.apply(get, "headers:" + key, {
          valueEncoding: "json"
        }),
        data: async.apply(get, "data:" + key),
        md5: async.apply(get, "md5:" + key, {
          valueEncoding: "utf8"
        })
      }, done);
    },
    function(results, done) {
      var headers = results.headers,
          data = results.data,
          md5 = results.md5,
          hash = crypto.createHash("md5").update(data).digest().toString("hex");

      if (md5 !== hash) {
        return done(new Error(util.format("MD5 values don't match for %s", key)));
      }

      if (data == null) {
        return done(new Error("Tile does not exist"));
      }

      return done(null, data, headers);
    }
  ], callback);
};

LevelDB.prototype.putTile = function(zoom, x, y, data, headers, callback) {
  if (typeof(headers) === "function") {
    callback = headers;
    headers = {};
  }

  headers = headers || {};
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  // normalize header names
  var _headers = {};
  Object.keys(headers).forEach(function(x) {
    _headers[x.toLowerCase()] = headers[x];
  });

  var md5;

  if (_headers["content-md5"]) {
    md5 = _headers["content-md5"];
  } else {
    md5 = crypto.createHash("md5").update(data).digest().toString("hex");

    _headers["content-md5"] = md5;
  }

  var key = [zoom, x, y].join("/"),
      cargo = this.cargo;

  async.waterfall([
    async.apply(this.open.bind(this)),
    function(db, done) {
      var operations = [
        {
          type: "put",
          key: "md5:" + key,
          value: md5,
          valueEncoding: "utf8"
        },
        {
          type: "put",
          key: "headers:" + key,
          value: headers,
          valueEncoding: "json"
        },
        {
          type: "put",
          key: "data:" + key,
          value: data
        }
      ];

      cargo.push(operations, done);
    }
  ], function(err) {
    if (err) {
      console.warn(err.stack);
    }

    // if we wanted to track pending writes, we would do it here
  })

  return callback();
};

LevelDB.prototype.getInfo = function(callback) {
  return async.waterfall([
    async.apply(this.open.bind(this)),
    function(db, done) {
      return db.get("info", {
        valueEncoding: "json"
      }, done);
    }
  ], callback);
};

LevelDB.prototype.putInfo = function(info, callback) {
  callback = callback || function() {};

  return async.waterfall([
    async.apply(this.open.bind(this)),
    function(db, done) {
      return db.put("info", info, {
        valueEncoding: "json"
      }, done);
    }
  ], callback);
};

LevelDB.prototype.dropTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  var key = [zoom, x, y].join("/");

  return async.waterfall([
    async.apply(this.open.bind(this)),
    function(db, done) {
      var operations = [
        {
          type: "del",
          key: "md5:" + key
        },
        {
          type: "del",
          key: "headers:" + key
        },
        {
          type: "del",
          key: "data:" + key
        }
      ];

      // callback will be called once the operations have been flushed
      return this.cargo.push(operations, done);
    }
  ], callback);
};

LevelDB.prototype.open = function(callback) {
  callback = callback || function() {};

  return open(this.path, {
    format: this.format,
    id: this.id,
    scale: this.scale
  }, callback);
};

LevelDB.prototype.close = function(callback) {
  callback = callback || function() {};

  if (!this.cargo.idle()) {
    this.cargo.drain = callback;
    return;
  }

  return callback();
};

LevelDB.registerProtocols = function(tilelive) {
  tilelive.protocols["leveldb+file:"] = this;
};

module.exports = function(tilelive, options) {
  LevelDB.registerProtocols(tilelive);

  return LevelDB;
};
