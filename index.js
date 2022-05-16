const moment = require('moment');
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const storage = new Storage();
const extractCSVJob = (bucketName = "", fileName = "", dataSet = "", table = "") => {
    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    // Run the query as a job
    const [job] = await bigquery
        .dataset(dataSet)
        .table(table)
        .extract(storage.bucket(bucketName).file(fileName));

    console.log(`Job ${job.id} created.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
        throw errors;
    }
}
const query = async () => {
    const dataSet = "ampl";
    const table = "mikmak_retailers";
    const bucketName = "bucket-mikmak-data-project";
    const rowsCSV = "mikmak-retailers.csv";
    // Queries the U.S. given names dataset for the state of Texas.
    // const query = `SELECT *
    //   FROM ${dataSet}.${table}
    //   LIMIT 1`;



    const isBucket = await checkBucketExistence(bucketName);
    console.log("is bucket exist---", isBucket);
    if (!isBucket) {
        const bucketAdded = await createBucket(bucketName);
        if (bucketAdded) {
            extractCSVJob(bucketName, rowsCSV, dataSet, table)
        }
    } else{
        extractCSVJob(bucketName, rowsCSV, dataSet, table)
    }

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
        console.log(`error occured during creating bucket name = ${name} and error is:-`, error);
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
const bucketCrud = (isBucket = false, bucketName = "", fileName = "", content = "") => {
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
}
exports.helloPubSub = async (event, context) => {
    // const bucketName = "bucket-mikmak-data-project-hello-world";
    // const fileName = "logs.txt";
    // const content = `lastTimestamp=${context.timestamp}`
    const message = event.data
        ? Buffer.from(event.data, 'base64').toString()
        : 'Hello, World';
    console.log("message is---", message);
    // const isBucket = await checkBucketExistence(bucketName);
    // bucketCrud(isBucket, bucketName, fileName, content);
    query();
};