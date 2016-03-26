"use strict";

var crypto = require("crypto"),
    path = require("path"),
    stream = require("stream"),
    url = require("url");

var async = require("async"),
    level = require("level");

var LevelDB = function(uri, callback) {
  if (typeof(uri) === "string") {
    uri = url.parse(uri, true);
  }

  this.uri = uri;
  this.path = path.join(uri.hostname, uri.pathname);

  // TODO scale (to support multi-scale archives)
  // TODO format (to support multi-format archives)

  return this.open(function(err) {
    if (err) {
      return callback(err);
    }

    return callback(null, this);
  }.bind(this));
};

LevelDB.prototype.createReadStream = function(options) {
  // options semantics borrowed from LevelUP
  // options.start
  // options.end
  // options.bbox
  // options.* - options that get passed along with coordinates to getTile()
  options.keys = "keys" in options ? options.keys : true;
  options.values = "values" in options ? options.values : true;
  options.limit = "limit" in options ? options.limit : -1;

  throw new Error("Not implemented: createReadStream()");
};

LevelDB.prototype.createKeyStream = function(options) {
  options.keys = true;
  options.values = false;

  return this.createReadStream(options);
};

LevelDB.prototype.createWriteStream = function() {
  throw new Error("Not implemented: createWriteStream()");
};

LevelDB.prototype.getTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
  zoom = zoom | 0;
  x = x | 0;
  y = y | 0;

  var tileStream = new stream.PassThrough(),
      dests = [],
      _pipe = tileStream.pipe,
      db = this.db;

  tileStream.pipe = function(dest) {
    dests.push(dest);

    return _pipe.apply(this, arguments);
  };

  tileStream.zoom = zoom;
  tileStream.x = x;
  tileStream.y = y;

  if (!db) {
    var err = new Error("Archive doesn't exist: " + url.format(this.uri));
    tileStream.emit("error", err);
    tileStream.end();

    return callback(err);
  }

  var coords = [zoom, x, y].join("/"),
      // TODO incorporate all options into the key (when options replace (z,x,y))
      key = "coords:" + coords;

  db.get(key, {
    valueEncoding: "utf8"
  }, function(err, md5) {
    if (err) {
      tileStream.emit("error", err);
      tileStream.end();

      return callback(err);
    }

    return async.parallel({
      headers: async.apply(db.get.bind(db),
                           "headers:" + coords,
                           {
                             valueEncoding: "json"
                           }),
      data: async.apply(db.get.bind(db),
                        "data:" + md5)
    }, function(err, results) {
      if (err) {
        tileStream.emit("error", err);
        tileStream.end();

        return callback(err);
      }

      var headers = results.headers,
          data = results.data;

      headers["content-md5"] = md5;

      dests.forEach(function(dest) {
        // http.ServerResponse-compatible piping
        if (dest.setHeader) {
          Object.keys(headers).forEach(function(x) {
            dest.setHeader(x, headers[x]);
          });
        }
      });

      tileStream.emit("headers", headers);
      tileStream.end(data);

      return callback(null, data, headers);
    });
  });

  return tileStream;
};

LevelDB.prototype._putTile = function(zoom, x, y, data, headers, callback) {
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
    var hash = crypto.createHash("md5");

    hash.update(data);

    md5 = hash.digest().toString("hex");
  }

  // this is a redundant header, as it's present in the key
  delete _headers["content-md5"];

  var coords = [zoom, x, y].join("/"),
      // TODO incorporate all supported options in the key if options were
      // provided
      coordinatesKey = "coords:" + coords,
      headersKey = "headers:" + coords,
      dataKey = "data:" + md5;

  return this.openForWrite(function(err) {
    if (err) {
      return callback(err);
    }

    var db = this.db;

    return db.get(dataKey, function(err, value) {
      if (err && err.type !== "NotFoundError") {
        return callback(err);
      }

      var operations = [
        {
          type: "put",
          key: coordinatesKey,
          value: md5,
          valueEncoding: "utf8"
        },
        {
          type: "put",
          key: headersKey,
          value: headers,
          valueEncoding: "json"
        }
      ];

      if (!value) {
        // only write data if it wasn't already there
        operations.push({
          type: "put",
          key: dataKey,
          value: data
        });
      }

      return db.batch(operations, callback);
    });
  }.bind(this));
};

LevelDB.prototype.putTile = function(zoom, x, y) {
  // outputs: leveldb.getTile(0, 0, 0).pipe(leveldb.putTile())
  // TODO make headers an optional argument

  if (arguments.length > 0) {
    // callback style
    return this._putTile.apply(this, arguments);
  }

  var tileStream = new stream.Transform(),
      chunks = [],
      sink = this;

  tileStream
    .on("pipe", function(src) {
      // use strict checks to allow 0 values

      this.zoom = src.zoom === undefined ? zoom : src.zoom;
      this.x = src.x === undefined ? x : src.x;
      this.y = src.y === undefined ? y : src.y;
      this.headers = this.headers || {};
    })
    .on("headers", function(headers) {
      this.headers = headers;
    });

  tileStream._transform = function(chunk, encoding, callback) {
    chunks.push(chunk);

    return callback();
  };

  tileStream._flush = function(callback) {
    return sink._putTile(this.zoom, this.x, this.y, Buffer.concat(chunks), this.headers, callback);
  };

  tileStream.setHeader = function(name, value) {
    this.headers[name] = value;
  };

  return tileStream;
};

LevelDB.prototype.dropTile = function(zoom, x, y, callback) {
  callback = callback || function() {};
};

LevelDB.prototype.open = function(callback) {
  callback = callback || function() {};

  this.db = level(this.path, {
    createIfMissing: false,
    valueEncoding: "binary"
  }, function(err) {
    if (err) {
      if (err.type === "OpenError") {
        this.db = null;
      } else {
        return callback(err);
      }
    }

    return callback();
  }.bind(this));
};

LevelDB.prototype.openForWrite = function(callback) {
  callback = callback || function() {};

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

    this.db = level(this.path, {
      createIfMissing: true,
      valueEncoding: "binary"
    }, callback);
  }.bind(this));
};

// TODO openForBulkWrite

LevelDB.prototype.close = function(callback) {
  callback = callback || function() {};

  return this.db.close(callback);
};

// blob:<MD5>
// <key>:<MD5>

LevelDB.registerProtocols = function(tilelive) {
  tilelive.protocols["leveldb+file:"] = this;
};

module.exports = function(tilelive, options) {
  LevelDB.registerProtocols(tilelive);

  return LevelDB;
};
