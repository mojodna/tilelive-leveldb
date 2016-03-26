"use strict";

var async = require("async");

var LevelDB = require("./")({ protocols: {} });

new LevelDB("leveldb+file://./tiles", function(err, src) {
  // TODO assert that the tiles directory doesn't exist yet
  async.series([
    function(done) {
      return src.putTile(0, 0, 0, new Buffer("foo"), function(err) {
        if (err) {
          console.error(err.stack);
          throw err;
        }

        // TODO assert that the tiles directory was created

        console.log("Tile written");

        return done();
      });
    },
    function(done) {
      return src.getTile(0, 0, 0, function(err, data, headers) {
        console.log("getTile:", arguments);

        return done();
      });
    },
    function(done) {
      return src.putInfo({
        scheme: "xyz",
        minzoom: 10,
        maxzoom: 22
      }, done);
    },
    function(done) {
      return src.getInfo(function(err, info) {
        console.log("getInfo:", arguments);

        return done();
      });
    }
  ], function(err, data) {
    // console.log(arguments);
  });
});
