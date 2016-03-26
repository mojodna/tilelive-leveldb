var http = require("http");


var LevelDB = require("./")({ protocols: {} });

new LevelDB("leveldb+file://./tiles", function(err, src) {
  http.createServer(function (req, resp) {
    src.getTile(0, 0, 0).pipe(resp);
  }).listen(8080);
});


