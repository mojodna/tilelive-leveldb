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
      readable = new stream.Transform({
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
    }).map(function(range) {
      var start = range[0],
          end = range[1],
          iterator = source.db.iterator({
            start: "data:" + start.join("/"),
            end: "data:" + end.join("/"),
            keyAsBuffer: false
          });

      return _(function(push, next) {
        return iterator.next(function(err, key, value) {
          if (err == null && key == null && value == null) {
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
              y = parts.shift() | 0;
              out = new ts.TileStream(z, x, y);

          return source.db.get("headers:" + coords, {
            valueEncoding: "json"
          }, function(err, headers) {
            push(out);

            out.setHeaders(headers);

            out.end(value);

            return next();
          });
        });
      });
    }).sequence().pipe(readable);
  });

  return readable;
};

// TODO reconcile this with the default createWriteStream implementation in
// tilelive-streaming
LevelDB.prototype.createWriteStream = function(options) {
  var sink = this,
      writeStream = new ts.Collector(),
      writable = new ts.Writable(sink),
      infoReceived = false,
      done = function(err) {
        if (err) {
          throw err;
        }
      };

  options = options || {};
  options.info = options.info || {};

  var putInfo = function(info) {
    return sink.putInfo(ts.restrict(options.info, info), done);
  };

  writeStream.once("info", function(info) {
    infoReceived = true;
    options.info = _.extend(options.info, info);

    return putInfo(info);
  });

  writable.on("finish", function() {
    if (!infoReceived) {
      infoReceived = true;

      return putInfo(options.info, async.apply(writable.emit.bind(writable), "finish"));
    }

    return sink.close(function() {});
  });

  writeStream.pipe(writable);

  // return the head-end of the pipeline
  return writeStream;
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
  var key = [zoom, x, y].join("/");

  return async.parallel({
    headers: async.apply(this.db.get.bind(this.db), "headers:" + key, {
      valueEncoding: "json"
    }),
    data: async.apply(this.db.get.bind(this.db), "data:" + key),
    md5: async.apply(this.db.get.bind(this.db), "md5:" + key, {
      valueEncoding: "utf8"
    })
  }, function(err, results) {
    if (err) {
      return callback(err);
    }

    var headers = results.headers,
        data = results.data,
        md5 = results.md5,
        hash = crypto.createHash("md5").update(data).digest().toString("hex");

    if (md5 !== hash) {
      return callback(new Error(util.format("MD5 values don't match for %s", key)));
    }

    // TODO if data == null, pass an Exception

    return callback(null, data, headers);
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
  var self = this;

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

  return this.openForWrite(function(err) {
    if (err) {
      return callback(err);
    }

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

    return self.db.batch(operations, callback);
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
  var self = this;

  return this.openForWrite(function(err) {
    if (err) {
      return callback(err);
    }

    return self.db.put("info", info, {
      valueEncoding: "json"
    }, callback);
  });
};

LevelDB.prototype.dropTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  var key = [zoom, x, y].join("/"),
      db = this.db;

  return this.openForWrite(function(err) {
    if (err) {
      return callback(err);
    }

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

    return db.batch(operations, callback);
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
      // TODO play with blockSize
      createIfMissing: true,
      valueEncoding: "binary"
    }, function(err, db) {
      if (err) {
        return callback(err);
      }

      self.db = db;

      return callback();
    });
  });
};

// TODO openForBulkWrite w/ tuned options (writeBufferSize)

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
