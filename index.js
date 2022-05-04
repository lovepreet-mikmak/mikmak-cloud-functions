/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
require("dotenv").config();
const fs = require("fs");
const lastTimestamp = require("./constants.js").lastTimestamp;
console.log("lastTimestamp--", lastTimestamp);
exports.helloPubSub = (event, context) => {
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("timestamp---", lastTimestamp);
    console.log("message is---", message);
    if (lastTimestamp) {

        console.log("The difference is ---", new Date(context.timestamp) - new Date(lastTimestamp));
    } else {
        console.log("This is first Time execution call");
    }
    fs.writeFile("constants.js", `module.exports.lastTimestamp='${context.timestamp}';`, function (err, data) {
        if (err) {
            console.log("error occured while writing file--", err);
        } else {
            console.log("file written successfully--", data);
        }
    })

};
