/*
 * lib/run.js
 *
 */


var prompt = require("prompt");
var fs = require("fs-extra");

module.exports = function(options) {
  prompt.colors = false;
  prompt.message = "";
  prompt.delimiter = "";
  prompt.start();

  console.log("Options: ", options);

  prompt.confirm("Do you want to continue?", function(err, ans) {
    if (ans) {
      console.log("Done.");
    } else {
      console.log("Cancelled.");
    }
  });
};

