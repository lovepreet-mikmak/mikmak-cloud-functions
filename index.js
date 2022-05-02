/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
var a = 25;
 exports.helloPubSub = (event, context) => {
    const message = event.data
      ? Buffer.from(event.data, 'base64').toString()
      : 'Hello, World';
      console.log("event---", event, "timestamp---", context.timestamp);
    console.log("message is---", message);
    console.log("A is--", a);
  };
  