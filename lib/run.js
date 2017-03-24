/*
 * lib/run.js
 *
 */


var _ = require("lodash");
var async = require("async");

var prompt = require("prompt");
var fs = require("fs-extra");
var pg = require("pg");

module.exports = function(options) {
  prompt.colors = false;
  prompt.message = "";
  prompt.delimiter = "";
  prompt.start();

  //-- debug...
  //console.log("Options: ", options);

  prompt.confirm("Do you want to continue?", function(err, ans) {
    if (ans) {
      perform(options, function(err) {
        console.log("Done.");
      });
    } else {
      console.log("Cancelled.");
    }
  });
};

//==============================================================================

function perform(options, done) {
  var conString = "postgres://" + (options.connect || "");
  var client = new pg.Client(conString);

  async.auto({
    connect: function(next) {
      client.connect(next);
    },
    query: ["connect", function(result, next) {
      client.query("select * from " + options.schema + ".corelogs", function(err, res) {
        console.log(err, _.get(res, "rows", []));
        next(err);
      });
    }],
  }, function(err, result) {
    client.end();
    done(err);
  });
}

//------------------------------------------------------------------------------
//==============================================================================
