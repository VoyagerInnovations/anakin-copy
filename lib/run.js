/*
 * lib/run.js
 *
 */

var _ = require("lodash");
var async = require("async");
var util = require("util");

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
  if (options.debug) {
    console.log("Options: ", options);
  }

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
  var dbSchema = options.schema || "";
  var srcFile = options.from || "";

  //----------------------------------------------------------------------------

  function toSqlInsert(json) {
    var table = dbSchema + ".corelogs";
    var fields = "";
    var values = "";

    _.forEach(json, function(v, k) {
      fields += fields ? "," : "";
      fields += k;

      values += values ? "," : "";
      values += JSON.stringify(v);
    });

    var query = util.format("INSERT INTO %s (%s) values (%s)", table, fields, values);

    //-- debug...
    if (options.debug) {
      console.log("query:", query);
    }

    return query;
  }

  //----------------------------------------------------------------------------

  function connect(next) {
    client.connect(next);
  }

  //----------------------------------------------------------------------------

  function query(result, next) {
    client.query("select * from " + dbSchema + ".corelogs", function(err, res) {
      console.log(err, _.get(res, "rows", []));
      next(err);
    });
  }

  //----------------------------------------------------------------------------

  function iterate(result, next) {
    iterator(srcFile, function(item, lineNo) {
      //-- debug...
      if (options.debug) {
        if (_.isEmpty(item)) {
          console.log("Empty!");
        } else {
          console.dir(item, {depth: null});
        }
      }

      if (!_.isEmpty(item)) {
        var query = toSqlInsert(item);
      }
    }, next);
  }

  //----------------------------------------------------------------------------

  function complete(err, result) {
    client.end();
    done(err);
  }

  //----------------------------------------------------------------------------
  //-- execution here...

  async.auto({
    connect: connect,
    query: ["connect", query],
    iterate: ["query", iterate]
  }, complete);
}

//==============================================================================
