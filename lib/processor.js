/*
 * lib/processor.js
 *
 */

var _ = require("lodash");
var path = require("path");
var async = require("async");
var util = require("util");
var pg = require("pg");
var fs = require("fs-extra");

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

  // var conString = "postgres://" + (_.get(self._options, "connect") || "");
  var destTable = _.get(self._options, "to") || "";
  var ignoreAttrs = _.get(self._options, "ignoreAttrs") || "";
  var toIgnore = ignoreAttrs ? _.split(ignoreAttrs, ",") : [];

  //
  // var client = new pg.Client(conString);

  async.series({
    // connected: function(next) {
    //   client.connect(function(err) {
    //     next(err, !err);
    //   });
    // },
    iterator: function(next) {
      iterator(file, handler, next);
    }
  }, function(err, result) {
    // if (result.connected) {
    //   client.end();
    // }
    done(err, result);
  });

  function handler(srcFile, lineNo, line, next) {
    //-- debug...
    self._options.debug && console.log("line %d:", lineNo, line);

    if (_.isEmpty(line)) return next();

    var json = {};
    try {
      var items = line.match(/{"(.*?)"}/g);
      json = JSON.parse(items);
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

    var newPath = _.replace(srcFile, ".log", "_output.sql");
    fs.appendFile(newPath, query+"\n", function(err, result) {
      if (err) {
        return next(err);
      }
      next();
    });
    //-- debug...
    self._options.debug && console.log("query:", query);

    if (_.isEmpty(query)) return next();
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

  var csgObj = {
    msg_id:row.MsgId,
    gateway_id:row.GatewayId,
    gsm_num:row.Phone,
    access_code:row.ShortCode,
    suffix:row.Suffix,
    msg:row.Body,
    status:row.status,
    result:row.Result,
    message_type:row.MessageType,
    dcs:0,
    tariff:row.Tariff,
    svc_desc:row.SvcDesc,
    err_code:row.ErrCode,
    rrn:row.Rrn,
    svc_id:row.SvcId,
    csg_cp_id:row.CSGCpId,
    csg_session_id:row.CSGSessionId,
    csg_trans_id:row.CSGTransId,
    csg_msg_type:row.CSGMsgType,
    csg_msg_subtype:row.CSGSubType,
    csg_service:row.CSGService,
    csg_subservice:row.CSGSubservice,
    operator:row.CarrierCode,
    err_txt:row.ErrText,
    csg_tariff:row.CSGTariffCode,
    client_status:row.client_status,
    datesent:row.datesent,
    timesent:row.timesent,
    process_times:row.process_times,
    scts:row.Scts,
  };

  _.forEach(csgObj, function(v, k) {
    fields += fields ? "," : "";
    fields += k;

    var val = util.format("%s", (_.isObject(v) ? JSON.stringify(v) : v));
    values += values ? "," : "";
    values += "'" + val.replace(/'/g, "''") + "'";
  });

  return util.format("INSERT INTO %s (%s) VALUES (%s);", table, fields, values);
}

//==============================================================================
