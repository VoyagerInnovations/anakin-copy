/*
 * lib/run.js
 *
 */

var _ = require("lodash");
var async = require("async");

var prompt = require("prompt");
var fs = require("fs-extra");
var pg = require("pg");

var iterator = require("./iterator");

//==============================================================================

module.exports = function(options) {

  prompt.colors = false;
  prompt.message = "";
  prompt.delimiter = "";
  prompt.start();

  //-- debug...
  //console.log("Options: ", options);

  function done(err, ans) {
    if (ans) {
      perform(options, function(err) {
        if (err) {
          console.log("Error:", err);
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
    prompt.confirm("Do you want to continue?", done);
  }

};

//==============================================================================

function perform(options, done) {
  var conString = "postgres://" + (options.connect || "");
  var client = new pg.Client(conString);

  function connect(next) {
    client.connect(next);
  }

  function query(result, next) {
    client.query("select * from " + options.schema + ".corelogs", function(err, res) {
      console.log(err, _.get(res, "rows", []));
      next(err);
    });
  }

  function iterate(result, next) {
    iterator(options.from, function(item) {
      if (_.isEmpty(item)) {
        console.log("Empty!");
      } else {
        console.dir(item, {depth: null});
      }
    }, next);
  }

  function complete(err, result) {
    client.end();
    done(err);
  }

  //-- execution here...
  async.auto({
    connect: connect,
    query: ["connect", query],
    iterate: ["query", iterate]
  }, complete);
}

//------------------------------------------------------------------------------
//==============================================================================
