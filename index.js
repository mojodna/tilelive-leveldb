"use strict";

var crypto = require("crypto"),
    path = require("path"),
    stream = require("stream"),
    url = require("url");

var _ = require("highland"),
    async = require("async"),
    level = require("level"),
    mercator = new (require("sphericalmercator"))(),
    ts = require("tilelive-streaming");

// TODO store db instances outside of individual objects (in a locking cache) so
// that multiple LevelDB instances can be open simultaneously providing
// different formats and scales

var LevelDB = function(uri, callback) {
  if (typeof(uri) === "string") {
    uri = url.parse(uri, true);
  }

  uri.query = uri.query || {};

  this.uri = uri;
  this.path = path.join(uri.hostname, uri.pathname);
  // support for multi-scale/multi-format archives
  this.scale = uri.query.scale || 1;
  this.format = uri.query.format || "";

  return this.open(function(err) {
    if (err) {
      return callback(err);
    }

    return callback(null, this);
  }.bind(this));
};

LevelDB.prototype.createReadStream = function(options) {
  var source = this,
      readable = new stream.PassThrough({
        objectMode: true
      });

  source.getInfo(function(err, info) {
    if (err) {
      return tileSource.emit("error", err);
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
      var iterator = source.db.db.iterator({
        gte: "tile:" + range[0].join("/"),
        lt: "tile:" + range[1].join("/"),
        keyAsBuffer: false,
        valueAsBuffer: false
      });

      return _(function(push, next) {
        return iterator.next(function(err, key, md5) {
          if (err == null && key == null && md5 == null) {
            return push(null, _.nil);
          }

          if (err) {
            push(err);

            return next();
          }

          var coords = key.split(":", 2).pop(),
              parts = coords.split("/"),
              z = parts.shift() | 0,
              x = parts.shift() | 0,
              y = parts.shift() | 0,
              out = new ts.TileStream(z, x, y);

          return async.parallel({
            data: async.apply(source.db.get.bind(source.db), "data:" + md5),
            headers: async.apply(source.db.get.bind(source.db), "headers:" + md5, {
              valueEncoding: "json"
            })
          }, function(err, result) {
            if (err) {
              push(err);

              return next();
            }

            push(null, out);

            out.setHeaders(result.headers);

            out.end(result.data);

            return next();
          });
        });
      });
    }).sequence().pipe(readable);
  });

  return readable;
};

LevelDB.prototype.getTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  if (!this.db) {
    return callback(new Error("Archive doesn't exist: " + url.format(this.uri)));
  }

  // TODO incorporate all options into the key (when this.options replace (z,x,y))
  var key = [zoom, x, y].join("/"),
      db = this.db;

  return db.get("tile:" + key, {
    valueEncoding: "utf8"
  }, function(err, md5) {
    return async.parallel({
      headers: async.apply(db.get.bind(db), "headers:" + md5, {
        valueEncoding: "json"
      }),
      data: async.apply(db.get.bind(db), "data:" + md5)
    }, function(err, results) {
      if (err) {
        return callback(err);
      }

      var headers = results.headers,
          data = results.data,
          hash = crypto.createHash("md5").update(data).digest().toString("hex");

      if (md5 !== hash) {
        return callback(new Error(util.format("MD5 values don't match for %s", key)));
      }

      // TODO if data == null, pass an Exception

      return callback(null, data, headers);
    });
  });
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

  // TODO incorporate all supported options in the key if options were
  // provided
  var key = [zoom, x, y].join("/");

  return this.openForWrite(function(err, db) {
    if (err) {
      return callback(err);
    }

    return db.get(md5, function(err, refs) {
      if (err && err.name !== "NotFoundError") {
        return callback(err);
      }

      var operations = [
        {
          type: "put",
          key: "tile:" + key,
          value: md5,
          valueEncoding: "utf8"
        },
        {
          type: "put",
          key: "headers:" + md5,
          value: headers,
          valueEncoding: "json"
        },
        {
          type: "put",
          key: md5,
          value: (refs | 0) + 1,
          valueEncoding: "json"
        }
      ];

      if (refs == null) {
        operations.push({
          type: "put",
          key: "data:" + md5,
          value: data
        });
      }

      return db.batch(operations, callback);
    });
  });
};

LevelDB.prototype.getInfo = function(callback) {
  if (!this.db) {
    return callback(new Error("Archive doesn't exist: " + url.format(this.uri)));
  }

  return this.db.get("info", {
    valueEncoding: "json"
  }, callback);
};

LevelDB.prototype.putInfo = function(info, callback) {
  callback = callback || function() {};

  return this.openForWrite(function(err, db) {
    if (err) {
      return callback(err);
    }

    return db.put("info", info, {
      valueEncoding: "json"
    }, callback);
  });
};

LevelDB.prototype.dropTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  var key = [zoom, x, y].join("/");

  return this.openForWrite(function(err, db) {
    if (err) {
      return callback(err);
    }

    return db.get("tile:" + key, {
      valueEncoding: "utf8"
    }, function(err, md5) {
      if (err) {
        return callback(err);
      }

      var operations = [
        // TODO use reference counting on [md5] value to determine when to drop
        // data:[md5]
        // {
        //   type: "del",
        //   key: "data:" + md5
        // },
        {
          type: "del",
          key: "headers:" + md5
        },
        {
          type: "del",
          key: key
        }
      ];

      return db.get(md5, {
        valueEncoding: "json"
      }, function(err, refs) {
        if (err && err.name !== "NotFoundError") {
          return callback(err);
        }

        refs = refs | 0;

        if (refs - 1 === 0) {
          operations.push({
            type: "del",
            key: md5
          }).push({
            type: "del",
            key: "data:" + md5
          });
        } else {
          operations.push({
            type: "put",
            key: md5,
            value: refs - 1,
            valueEncoding: "json"
          });
        }

        return db.batch(operations, callback);
      });
    });
  });
};

LevelDB.prototype.open = function(callback) {
  callback = callback || function() {};
  var self = this;

  this.db = level(this.path, {
    // TODO play with blockSize
    // TODO extract options
    createIfMissing: false,
    valueEncoding: "binary"
  }, function(err) {
    if (err) {
      if (err.type === "OpenError") {
        self.db = null;
      } else {
        return callback(err);
      }
    }

    return callback();
  });
};

LevelDB.prototype.openForWrite = function(callback) {
  callback = callback || function() {};
  var self = this;

  var close = function(cb) {
    return cb();
  };

  if (this.db) {
    // close and re-open if appropriate
    close = this.db.close.bind(this.db);
  }

  return close(function(err) {
    if (err) {
      return callback(err);
    }

    return level(self.path, {
      createIfMissing: true,
      valueEncoding: "binary"
    }, function(err, db) {
      if (err) {
        return callback(err);
      }

      self.db = db;

      return callback(null, db);
    });
  });
};

LevelDB.prototype.close = function(callback) {
  callback = callback || function() {};

  return this.db.close(callback);
};

LevelDB.registerProtocols = function(tilelive) {
  tilelive.protocols["leveldb+file:"] = this;
};

module.exports = function(tilelive, options) {
  LevelDB.registerProtocols(tilelive);

  return LevelDB;
};
