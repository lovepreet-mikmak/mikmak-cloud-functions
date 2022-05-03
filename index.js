/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
var lastTimestamp = process.env.lastTimestamp;
exports.helloPubSub = (event, context) => {
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("event---", event, "timestamp---", context.timestamp);
    console.log("message is---", message);
    if (lastTimestamp) {
        console.log("The difference is ---", new Date(context.timestamp)- new Date(lastTimestamp));
    } else{
        console.log("This is first call");
    }
    process.env.lastTimestamp = context.timestamp;
    
};
