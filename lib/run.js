/*
 * lib/run.js
 *
 */

var _ = require("lodash");
var async = require("async");
var util = require("util");
var prompt = require("prompt");
var pg = require("pg");

var Datasource = require("./datasource");
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
    var promptMsg = util.format("You are about to import data from '%s' to '%s' table.\nDo you want to continue?", 
      options.from, options.to
    );
    prompt.confirm(promptMsg, done);
  }

};

//==============================================================================

function perform(options, done) {
  var conString = "postgres://" + (_.get(options, "connect") || "");
  var destTable = _.get(options, "to") || "";
  var ignoreAttrs = _.get(options, "ignoreAttrs") || "";
  var toIgnore = ignoreAttrs ? _.split(ignoreAttrs, ",") : [];

  var datasource = new Datasource(options);
  var client = new pg.Client(conString);

  //----------------------------------------------------------------------------

  function connect(next) {
    client.connect(next);
  }

  //----------------------------------------------------------------------------

  function walk(result, next) {
    datasource.getFiles(next);
  }

  //----------------------------------------------------------------------------

  function iterate(result, next) {
    var files = _.get(result, "files", []);

    async.eachSeries(files, function(file, next) {
      console.log("Processing file '%s'", file);

      iterator(file, handler, function(err) {
        //-- we just log the error...
        err && console.log("Processing error, file '%s':", file, err);
        next();
      });
    }, next);

    function handler(srcFile, lineNo, line, next) {
      //-- debug...
      options.debug && console.log("line %d:", lineNo, line);

      if (_.isEmpty(line)) return next();

      var json = {};
      try {
        json = JSON.parse(line);
      } catch (e) {
        if (options.debug) {
          console.log("Parse error, file '%s': line %d:", srcFile, lineNo, e.message, line);
        } else {
          console.log("Parse error, file '%s': line %d:", srcFile, lineNo, e.message);
        }
      }

      if (_.isEmpty(json)) return next();

      //-- set identifiers...
      _.set(json, "srcfile", srcFile);
      _.set(json, "lineno", lineNo);

      var query = toSqlInsert(destTable, _.omit(json, toIgnore));

      //-- debug...
      options.debug && console.log("query:", query);

      if (_.isEmpty(query)) return next();

      client.query(query, function(err, res) {
        if (err) {
          if (options.debug) {
            console.log("Query error, file '%s': line %d:",
              srcFile, lineNo, (_.isError(err) ? err.message : err), query);
          } else {
            console.log("Query error, file '%s': line %d:",
              srcFile, lineNo, (_.isError(err) ? err.message : err));
          }
        }
        next();
      });
    }
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
    files: ["connect", walk],
    iterate: ["files", iterate]
  }, complete);
}

//==============================================================================
//-- helpers...

function toSqlInsert(table, row) {
  var fields = "";
  var values = "";

  if (_.isEmpty(row) || !_.isObject(row)) {
    return "";
  }

  _.forEach(row, function(v, k) {
    fields += fields ? "," : "";
    fields += k;

    var val = util.format("%s", (_.isObject(v) ? JSON.stringify(v) : v));

    values += values ? "," : "";
    values += "'" + val.replace(/'/g, "''") + "'";
  });

  return util.format("INSERT INTO %s (%s) values (%s);", table, fields, values);
}

//==============================================================================
