/*
 * lib/run.js
 *
 */

var _ = require("lodash");
var async = require("async");
var util = require("util");
var path = require("path");

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
    var promptMsg = util.format("You are about to import logs from '%s' to '%s' table.\nDo you want to continue?", 
      options.from, options.to
    );
    prompt.confirm(promptMsg, done);
  }

};

//==============================================================================

function perform(options, done) {
  var conString = "postgres://" + (options.connect || "");
  var client = new pg.Client(conString);
  var dbSchema = options.schema || "";
  var srcFile = options.from || "";
  var destTable = options.to || "";
  var srcFileBasename = path.basename(srcFile);

  //----------------------------------------------------------------------------

  function toSqlInsert(obj) {
    var fields = "";
    var values = "";

    options.debug && console.dir(obj);

    if (_.isEmpty(obj) || !_.isObject(obj)) {
      return "";
    }

    _.forEach(obj, function(v, k) {
      fields += fields ? "," : "";
      fields += k;

      var val = util.format("%s", (_.isObject(v) ? JSON.stringify(v) : v));

      values += values ? "," : "";
      values += "'" + val.replace(/'/g, "''") + "'";
    });

    return util.format("INSERT INTO %s (%s) values (%s);", destTable, fields, values);
  }

  //----------------------------------------------------------------------------

  function connect(next) {
    client.connect(next);
  }

  //----------------------------------------------------------------------------

  function iterate(result, next) {
    iterator(srcFile, function(lineNo, line, next) {
      //-- debug...
      options.debug && console.log("line %d:", lineNo, line);

      if (_.isEmpty(line)) return next();

      var json = {};
      try {
        json = JSON.parse(line);
      } catch (e) {
        console.log("Parse error, file: '%s': line %d:", srcFile, lineNo, e);
      }

      //-- debug...
      if (_.isEmpty(json)) return next();

      //-- set identifiers...
      _.set(json, "v_sf", srcFileBasename);
      _.set(json, "v_sfln", lineNo);

      var query = toSqlInsert(json);

      //-- debug...
      options.debug && console.log("query:", query);

      if (_.isEmpty(query)) return next();

      client.query(query, function(err, res) {
        if (err) {
          console.log("Query error, file '%s': line %d: query: %s",
            srcFile, lineNo, query, err);
        }
        next();
      });
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
    iterate: ["connect", iterate]
  }, complete);
}

//==============================================================================
