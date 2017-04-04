/*
 * lib/processor.js
 *
 */

var _ = require("lodash");
var path = require("path");
var async = require("async");
var util = require("util");
var pg = require("pg");

var iterator = require("./iterator");

//-- export
module.exports = Processor;

//==============================================================================

function Processor(options) {
  var _options = _.merge({}, options);

  Object.defineProperty(this, "_options", {
    get: function() {
      return _options;
    }
  });
}

//------------------------------------------------------------------------------

Processor.prototype.import = function(file, done) {
  var self = this;

  var conString = "postgres://" + (_.get(self._options, "connect") || "");
  var destTable = _.get(self._options, "to") || "";
  var ignoreAttrs = _.get(self._options, "ignoreAttrs") || "";
  var toIgnore = ignoreAttrs ? _.split(ignoreAttrs, ",") : [];

  var client = new pg.Client(conString);

  async.series({
    connected: function(next) {
      client.connect(function(err) {
        next(err, !err);
      });
    },
    iterator: function(next) {
      iterator(file, handler, next);
    }
  }, function(err, result) {
    if (result.connected) {
      client.end();
    }
    done(err);
  });

  function handler(srcFile, lineNo, line, next) {
    //-- debug...
    self._options.debug && console.log("line %d:", lineNo, line);

    if (_.isEmpty(line)) return next();

    var json = {};
    try {
      json = JSON.parse(line);
    } catch (e) {
      if (self._options.debug) {
        console.log("Parse error, file '%s': line %d:", srcFile, lineNo, e.message, line);
      } else {
        console.log("Parse error, file '%s': line %d:", srcFile, lineNo, e.message);
      }
    }

    if (_.isEmpty(json)) return next();

    var absLocalDir = path.resolve(self._options.localDir);
    var inLocalDir = !!srcFile.match("^" + absLocalDir);
    var relSrcFile = path.relative(self._options.localDir, srcFile);

    //-- set identifiers...
    _.set(json, "srcfile", (inLocalDir ? relSrcFile : srcFile));
    _.set(json, "lineno", lineNo);

    var query = toSqlInsert(destTable, _.omit(json, toIgnore));

    //-- debug...
    self._options.debug && console.log("query:", query);

    if (_.isEmpty(query)) return next();

    client.query(query, function(err, res) {
      if (err) {
          console.log("Query error, file '%s': line %d:", srcFile, lineNo,
            (_.isError(err) ? err.message : err),
            (self._options.debug ? query : ""));
      }
      next();
    });
  }
};

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
