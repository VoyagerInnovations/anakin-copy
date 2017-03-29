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

    var lineReader = readline.createInterface({ input: input });
  } catch (e) {
    //-- something went wrong!
    return done(e);
  }

  //-- initialize
  var lineNo = 0;
  var processedLines = 0;
  var doneReading = false;
  var doneProcessing = false;
  var paused = false;
  var minDiff = 5;
  var maxDiff = 10;

  //-- trottling
  function trottle() {
    var diff = lineNo - processedLines;

    if (paused) {
      if (diff < minDiff) {
        paused = false;
        lineReader.resume();
      }
    } else {
      if (diff > maxDiff) {
        paused = true;
        lineReader.pause();
      }
    }
  }

  //-- callback
  function next() {
    if (!doneProcessing && doneReading && processedLines >= lineNo) {
      //-- done processing
      doneProcessing = true;
      done();
    } else {
      trottle();
    }
  }

  //-- process line
  lineReader.on("line", function(line) {
    ++lineNo;
    handler(fpath, lineNo, _.trim(line), function(err) {
      ++processedLines;
      next();
    });
    trottle();
  });

  //-- done reading file
  lineReader.on("close", function() {
    doneReading = true;
    //-- this is important !!! :)
    next();
  });

};

//==============================================================================

function isGzip(fpath) {
  var buffer = readChunk.sync(fpath, 0, 4100);
  var ftype = fileType(buffer);

  return (_.get(ftype, "mime") === "application/gzip");
}

//------------------------------------------------------------------------------
//==============================================================================
