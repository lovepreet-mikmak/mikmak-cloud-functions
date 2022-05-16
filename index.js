const moment = require('moment');
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const storage = new Storage();
const query = async () => {
    // Queries the U.S. given names dataset for the state of Texas.
    const query = `SELECT *
      FROM ampl.mikmak_retailers
      LIMIT 100`;

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options = {
        query: query,
        // Location must match that of the dataset(s) referenced in the query.
        location: 'US',
    };

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();

    // Print the results
    console.log('Rows:');
    rows.forEach(row => console.log(row));
}
const checkBucketExistence = async (name = "") => {
    try {
        const [buckets] = await storage.getBuckets();
        return buckets.findIndex(bucket => bucket.name === name) >= 0 ? true : false;
    }
    catch (error) {
        console.log("error occured during fetching buckets list:-", error);
        return false;
    }
};
const createBucket = async (name = "") => {
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

    // console.log("Files:", files);
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
};
const readFile = async (bucketName = "", fileName = "") => {
    try {
        // Downloads the file into a buffer in memory.
        const contents = await storage.bucket(bucketName).file(fileName).download();

        console.log(`Contents of gs://${bucketName}/${fileName} are ${contents.toString()}`);
        const index = contents.toString().indexOf("lastTimestamp=");
        if (index >= 0) {
            return contents.toString().slice(index + 14);
        } else {
            return null;
        }

    } catch (error) {
        console.log("error while reading remote file:-", error);
        return false;
    }
}

exports.helloPubSub = async (event, context) => {
    const bucketName = "bucket-mikmak-event-api-hello-world";
    const fileName = "logs.txt";
    const content = `lastTimestamp=${context.timestamp}`
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("message is---", message);
    query();
    const isBucket = await checkBucketExistence(bucketName);
    if (!isBucket) {
        const bucketAdded = await createBucket(bucketName);
        if (bucketAdded) {
            createFile(bucketName, fileName, content);
        }
    }
    else {
        const isFile = await checkFileExistence(bucketName, fileName);
        if (!isFile) {
            createFile(bucketName, fileName, content);
        } else {
            const oldContent = await readFile(bucketName, fileName);
            if (oldContent) {
                const diff = moment(oldContent).from(moment(context.timestamp))
                console.log("Time Interval since last Trigger---", diff);
            }
            else {
                console.log(`This is the first time,when ${fileName} file is created`);
            }
            updateFile(bucketName, fileName, content);
        }
    }
};