/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
require("dotenv").config();
const fs = require("fs");
const path = require("path");
console.log("process.env--",process.env.lastTimestamp);
const lastTimestamp = process.env.lastTimestamp;
exports.helloPubSub = (event, context) => {
    const filePath =  path.resolve(__dirname,"/.env");
    console.log("filePath--", filePath);
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("event---", event, "timestamp---", context.timestamp);
    console.log("message is---", message);
    if (lastTimestamp) {

        console.log("The difference is ---", new Date(context.timestamp) - new Date(lastTimestamp));
    } else{
        console.log("This is first Time execution call");
    }
    fs.writeFile(filePath, `lastTimestamp=${context.timestamp}`, function(err, data){
        if(err){
            console.log("error occured while writing file--", err);
        } else{
            console.log("file written successfully--", data);
        }
    })
    
};
