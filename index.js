#!/usr/bin/env node

var fs = require("fs-extra");
var package = require("./package.json");

var program = require("commander").command(package.name);
var prompt = require("prompt");
var args = process.argv.slice(2);

program
  .version(package.version, "-v, --version")
  .option("-o, --option <option>", "Sample option.")
  .parse(process.argv);

!args.length && program.help();

run();

function run() {
  prompt.colors = false;
  prompt.message = "";
  prompt.delimiter = "";
  prompt.start();

  console.log("Hello world! option: %s", program.option);

  prompt.confirm("Do you want to continue?", function(err, ans) {
    if (ans) {
      console.log("Done.");
    } else {
      console.log("Cancelled.");
    }
  });
}
