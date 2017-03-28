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

module.exports = function(fpath, handler, done) {

  try {
    var input = fs.createReadStream(fpath);

    if (isGzip(fpath)) {
      input = input.pipe(zlib.createGunzip());
    }

    //-- initialize
    var lineReader = readline.createInterface({ input: input });
    var lineNo = 0;
    var processedLines = 0;
    var completed = false;
    var paused = false;
    var maxDiff = 10;

    function trottle() {
      var diff = lineNo - processedLines;

      if (paused) {
        if (diff < maxDiff) {
          paused = false;
          lineReader.resume();
        }
      } else {
        if (diff >= maxDiff) {
          paused = true;
          lineReader.pause();
        }
      }
    }

    function next() {
      //console.log("lineNo: %d, processedLines: %d", lineNo, processedLines);
      trottle();
      if (completed && processedLines >= lineNo) {
        done();
      }
    }

    //-- process line
    lineReader.on("line", function(line) {
      ++lineNo;
      trottle();
      handler(lineNo, _.trim(line), function(err) {
        ++processedLines;
        next();
      });
    });

    //-- done processing
    lineReader.on("close", function() {
      completed = true;
    });
  } catch (e) {
    //-- something went wrong!
    done(e);
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
