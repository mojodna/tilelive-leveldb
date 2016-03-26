var LevelDB = require("./")({ protocols: {} });

new LevelDB("leveldb+file://./tiles", function(err, src) {
  // TODO assert that the tiles directory doesn't exist yet
  // return src.putTile(0, 0, 0, new Buffer("foo"), {}, function(err) {
  //   if (err) {
  //     console.error(err.stack);
  //     throw err;
  //   }

  //   // TODO assert that the tiles directory was created

  //   console.log("Tile written");
  // });

  // return src.getTile(0, 0, 0, function(err, data, headers) {
  //   console.log(arguments);
  // });

  src.getTile(0, 0, 0)
    .on("headers", function() {
      console.log("headers:", arguments);
    })
    .on("data", function(chunk) {
      console.log(arguments);
    }).pipe(process.stdout);
});


