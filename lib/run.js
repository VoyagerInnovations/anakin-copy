/*
 * lib/run.js
 *
 */

var _ = require("lodash");
var async = require("async");
var util = require("util");
var prompt = require("prompt");

var Datasource = require("./datasource");
var Processor = require("./processor");

//==============================================================================

module.exports = function(options) {

  prompt.colors = false;
  prompt.message = "";
  prompt.delimiter = "";
  prompt.start();

  //-- debug...
  if (options.debug) {
    console.log("Options: ", options);
  }

  function done(err, ans) {
    if (ans) {
      perform(options, function(err) {
        if (err) {
          console.log("Error:", _.isError(err) ? err.message : err);
        } else {
          console.log("Done.");
        }
      });
    } else {
      console.log("Cancelled.");
    }
  }

  if (options.force) {
    done(null, true);
  } else {
    var promptMsg = util.format("You are about to import data from '%s' to '%s' table.\n" +
                                "Do you want to continue?", options.from, options.to);
    prompt.confirm(promptMsg, done);
  }

};

//==============================================================================

function perform(options, done) {
  async.auto({
    files: function(next) {
      var datasource = new Datasource(options);
      datasource.getFiles(next);
    },
    iterate: ["files", function(result, next) {
      var files = _.get(result, "files", []);

      async.eachLimit(files, 8, function(file, next) {
        console.log("Processing start: '%s'", file);
        var processor = new Processor(options);

        processor.import(file, function(err) {
          if (err) {
            //-- we just log the error...
            console.log("Processing error: '%s':", file, _.isError(err) ? err.message : err);
          }
          console.log("Processing end: '%s'", file);
          next();
        });
      }, next);
    }]
  }, done);
}

//==============================================================================
