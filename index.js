#!/usr/bin/env node

var _ = require("lodash");

var package = require("./package.json");
var run = require("./lib/run");

var program = require("commander").command(package.name);
var args = process.argv.slice(2);

//==============================================================================

program
  .version(package.version, "-v, --version")
  .option("--connect <connect>", "Db connection string")
  .option("--from <from>", "Source folder or file")
  .option("--to <to>", "Destination table")
  .option("--local-dir <local_dir>", "Local directory where remote files are synced, default=./tmp", "tmp")
  .option("--access-key <access_key>", "AWS access key")
  .option("--secret-key <secret_key>", "AWS secret key")
  .option("--ignore-attrs <attrs_list>", "Ignore attributes")
  .option("-p, --processors <processors>", "Max processors, default=4", function(v) {return _.toInteger(v)}, 4)
  .option("-r, --recursive [true|false]", "Recursive copy, default=false", function(v) {return v !== "false"}, false)
  .option("-f, --force [true|false]", "Non-interactive, default=false", function(v) {return v !== "false"}, false)
  .option("-d, --debug [true|false]", "Show debug messages, default=false", function(v) {return v !== "false"}, false)
  .parse(process.argv);

//-- show help for empty arguments...
!args.length && program.help();

//-- run the program...
run(program.opts());

//------------------------------------------------------------------------------
//-- handle uncaught exceptions...

//process.on("uncaughtException", function(e) {
//  console.log("Ooops, something went wrong!!!\n", e);
//  process.exit();
//})

//==============================================================================
