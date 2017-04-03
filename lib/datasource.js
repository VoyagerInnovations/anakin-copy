/*
 * lib/datasource.js
 *
 */

var _ = require("lodash");
var fs = require("fs-extra");
var path = require("path");
var util = require("util");
var async = require("async");
var walkSync = require("klaw-sync");
var http = require("http");
var https = require("https");
var s3 = require("s3");
var mm = require("micromatch");

//-- valid file extensions
var FILE_EXT = ["json", "log", "json.gz", "log.gz"];

//-- initialize global sockets
http.globalAgent.maxSockets = https.globalAgent.maxSockets = 20;

//-- export
module.exports = Datasource;

//==============================================================================

function Datasource(options) {
  var from = _.get(options, "from") || "";
  var fromS3 = from.match(/^s3:\/\/([^\/]+)(.*)$/);

  var localDir = _.get(options, "localDir") || "";
  var localPath = path.resolve(from);

  var bucket = "";
  var bucketPath = "";

  if (fromS3) {
    bucket = _.get(fromS3, "1") || "";
    bucketPath = _.trimStart(_.get(fromS3, "2") || "", "/");
    localPath = path.resolve(localDir, bucketPath);
  }

  var _options = _.merge({
    bucket: bucket,
    bucketPath: bucketPath,
    localPath: localPath
  }, options);

  Object.defineProperty(this, "_options", {
    get: function() {
      return _options;
    }
  });
}

//------------------------------------------------------------------------------

Datasource.prototype.getFiles = function(done) {
  var self = this;

  async.auto({
    download: function(next) {
      if (self._options.bucket) {
        download(self._options, next);
      } else {
        next();
      }
    },
    files: ["download", function(result, next) {
      walk(self._options, next);
    }]
  }, function(err, result) {
    var files = _.get(result, "files") || [];
    done(err, files);
  });
};

//==============================================================================
//-- helpers

function download(options, done) {
  var accessKey = _.get(options, "accessKey") || "";
  var secretKey = _.get(options, "secretKey") || "";
  var bucket = _.get(options, "bucket") || "";
  var bucketPath = _.get(options, "bucketPath") || "";
  var localPath = _.get(options, "localPath") || "";
  var pathIsDir = /.*\/$/.test(bucketPath);

  var downloadMethod = pathIsDir ? "downloadDir" : "downloadFile" ;
  var downloadOptions = pathIsDir ? {
    localDir: localPath,
    deleteRemoved: true,
    s3Params: {
      Bucket: bucket,
      Prefix: bucketPath
    }
  } : {
    localFile: localPath,
    s3Params: {
      Bucket: bucket,
      Key: bucketPath
    }
  };

  var client = s3.createClient({
    s3Options: {
      accessKeyId: accessKey,
      secretAccessKey: secretKey
    }
  });

  var downloader = client[downloadMethod](downloadOptions);

  downloader.on("error", function(err) {
    options.debug && console.log("download error:", err);
    done(err);
  });

  downloader.on("end", function() {
    options.debug && console.log("download done");
    console.log("File(s) were synced locally to '%s'", localPath);
    done();
  });

  downloader.on("fileDownloadStart", function(localFilePath, s3Key) {
    options.debug && console.log("download start:", localFilePath, s3Key);
  });

  downloader.on("fileDownloadEnd", function(localFilePath, s3Key) {
    options.debug && console.log("download end:", localFilePath, s3Key);
  });
}

//------------------------------------------------------------------------------

function walk(options, done) {
  var localPath = _.get(options, "localPath") || "";
  var recursive = !!_.get(options, "recursive");

  var files = [];
  var filter = util.format("*.{%s}", _.join(FILE_EXT, ","));
  var ignore = recursive ? ("**/!" + filter) : ("!" + filter);

  try {
    var localPathStat = fs.statSync(localPath);

    if (localPathStat.isFile()) {
      files = mm([localPath], filter, {matchBase: true});
      if (_.isEmpty(files)) {
        throw new Error("Invalid data source.");
      }
    } else if (localPathStat.isDirectory()) {
      files = _.map(walkSync(localPath, {
        nodir: true,
        ignore: ignore
      }), "path");
    } else {
      throw new Error("Invalid data source.");
    }
  } catch (e) {
    return done(e);
  }

  return done(null, files);
}

//==============================================================================
