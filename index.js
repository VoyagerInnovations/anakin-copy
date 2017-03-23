#!/usr/bin/env node

var package = require("./package.json");
var run = require("./lib/run");

var program = require("commander").command(package.name);
var args = process.argv.slice(2);

program
  .version(package.version, "-v, --version")
  .option("--from <from>", "Source folder or file")
  .option("--to <to>", "Destination table")
  .option("-r, --recursive [true|false]", "Recursive copy", function(v) {return v !== "false"}, false)
  .option("--access-key [access_key]", "Access key")
  .option("--secret-key [secret_key]", "Secret key")
  .parse(process.argv);

!args.length && program.help();

run(program.opts());

