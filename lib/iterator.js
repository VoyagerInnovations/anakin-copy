/*
 * lib/iterator.js
 *
 */

var _ = require("lodash");
var fs = require("fs-extra");
var zlib = require("zlib");
var readline = require("readline");
var readChunk = require("read-chunk");
var fileType = require("file-type");

//==============================================================================

module.exports = function(fpath, handler, callback) {

  try {
    var input = fs.createReadStream(fpath);

    if (isGzip(fpath)) {
      input = input.pipe(zlib.createGunzip());
    }

    //-- initialize
    var lineReader = readline.createInterface({ input: input });
    var lineNo = 0;

    //-- process line
    lineReader.on("line", function(line) {
      ++lineNo;
      try {
        var item = _.trim(line);
        handler((_.isEmpty(item) ? {} : JSON.parse(item)), lineNo);
      } catch (e) {
        console.log("Parse error, line %d:", lineNo, line);
      }
    });

    //-- done processing
    lineReader.on("close", function() {
      callback();
    });
  } catch (e) {
    //-- something went wrong!
    callback(e);
  }

};

//==============================================================================

function isGzip(fpath) {
  var buffer = readChunk.sync(fpath, 0, 4100);
  var ftype = fileType(buffer);

  return (_.get(ftype, "mime") === "application/gzip");
}

//------------------------------------------------------------------------------
//==============================================================================
