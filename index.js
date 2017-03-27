#!/usr/bin/env node

var package = require("./package.json");
var run = require("./lib/run");

var program = require("commander").command(package.name);
var args = process.argv.slice(2);

//==============================================================================

program
  .version(package.version, "-v, --version")
  .option("--connect <connect>", "Db connection string")
  .option("--schema <schema>", "Db schema")
  .option("--from <from>", "Source folder or file")
  .option("--to <to>", "Destination table")
  .option("--access-key <access_key>", "AWS access key")
  .option("--secret-key <secret_key>", "AWS secret key")
  .option("-r, --recursive [true|false]", "Recursive copy, default=false", function(v) {return v !== "false"}, false)
  .option("-f, --force [true|false]", "Non-interactive, default=false", function(v) {return v !== "false"}, false)
  .option("-d, --debug [true|false]", "Non-interactive, default=false", function(v) {return v !== "false"}, false)
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
