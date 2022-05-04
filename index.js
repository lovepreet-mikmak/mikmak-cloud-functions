/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
const { initializeApp, applicationDefault, cert } = require('firebase-admin/app');
const { getFirestore, Timestamp, FieldValue } = require('firebase-admin/firestore');
exports.helloPubSub = async (event, context) => {
    initializeApp();

    const db = getFirestore();
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("message is---", message);
    // const docRef = db.collection('logs').doc('timeStamp');

    // await docRef.set({
    //     timeStamp: "hello"
    // });
    const snapshot = await db.collection('logs').get();
    snapshot.forEach((doc) => {
        console.log(doc.id, '=>', doc.data());
    });
    // if (lastTimestamp) {

    //     console.log("The difference is ---", new Date(context.timestamp) - new Date(lastTimestamp));
    // } else {
    //     console.log("This is first Time execution call");
    // }


};
