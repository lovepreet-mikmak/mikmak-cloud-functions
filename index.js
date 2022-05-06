const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const checkBucketExistence = async (name = "") => {
    try {
        const [buckets] = await storage.getBuckets();

        console.log('Buckets:', buckets);
        return buckets.findIndex(bucket => bucket.name === name) >= 0 ? true : false;
    }
    catch (error) {
        console.log("error occured during fetching buckets list:-", error);
        return false;
    }
};
const createBucket = async (name = "", fileName = "", content = "") => {
    // Creates the new bucket
    try {
        await storage.createBucket(name);
        console.log(`Bucket ${name} created.`);
        return true;
    }
    catch (error) {
        console.log("error occured during creating bucket:-", error);
        return false;
    }
};
const checkFileExistence = async (bucketName = "", fileName = "") => {
    const [files] = await storage.bucket(bucketName).getFiles();

    console.log("Files:", files);
    return files.findIndex(file => file.name === fileName) >= 0 ? true : false;;
}
const createFile = async (bucketName = "", fileName = "", content = "") => {
    try {
        await storage.bucket(bucketName).file(fileName).save(content);

        console.log(
            `${fileName} with content ${content} uploaded to ${bucketName}.`
        );
        return true;

    } catch (error) {
        console.log("error while creating file:-", error);
        return false;
    }
};
const updateFile = async (bucketName = "", fileName = "", content = "") => {
    try {
        await storage.bucket(bucketName).file(fileName).delete();
        await storage.bucket(bucketName).file(fileName).save(content);
        console.log(`${fileName} updated`);

    } catch (error) {
        console.log("error while updating file:-", error);
        return false;
    }
}

exports.helloPubSub = (event, context) => {
    const bucketName = "bucket1";
    const fileName = "logs.txt";
    const content = `lastTimestamp=${context.timestamp}`
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("message is---", message);
    checkBucketExistence(bucketName);
    // const isBucket = checkBucketExistence(bucketName);
    // if (!isBucket) {
    //     const bucketAdded = createBucket(bucketName, fileName, content);
    //     if (bucketAdded) {
    //         createFile(bucketName, fileName, content);
    //     }
    // } else {
    //     const isFile = checkFileExistence(bucketName, fileName);
    //     if (!isFile) {
    //         createFile(bucketName, fileName, content);
    //     } else {
    //         updateFile(bucketName, fileName, content);
    //     }
    // }
};