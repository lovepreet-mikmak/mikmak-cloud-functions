const moment = require('moment');
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const storage = new Storage();
/**
 * This function will do the job of extracting data from database and store in a .csv file on google cloud storage
 * @param {*} bucketName The name of google storage bucket to be used
 * @param {*} fileName The name of file of that bucket to be used
 * @param {*} dataSet  The name of the databse in the project
 * @param {*} table  The name of the table inside that database
 */
const extractCSVJob = async (bucketName = "", fileName = "", dataSet = "", table = "") => {
    const query = `SELECT COUNT(*) From ${dataSet}.${table}
      `;

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
    const [result] = await job.getQueryResults();

    // Print the results
    console.log('result:', result);
    // rows.forEach(row => console.log("row is ---",row));
    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
        throw errors;
    }
}
/**
 * This is the main function for CSV Task that calls extractCSVJob() based upon conditional check on if bucket avaialble or not. if the bucket  doesn't exists then it will first create a new bucket and then perform extraction job
 */
const csvQuery = async () => {
    const dataSet = "ampl";
    const table = "mikmak_retailers";
    const bucketName = "bucket-mikmak-data-project";
    const fileName = "mikmak-retailers.csv";
    const isBucket = await checkBucketExistence(bucketName);
    if (!isBucket) {
        const bucketAdded = await createBucket(bucketName);
        if (bucketAdded) {
            extractCSVJob(bucketName, fileName, dataSet, table)
        }
    } else {
        extractCSVJob(bucketName, fileName, dataSet, table)
    }

}
/**
 * 
 * @param {*} name The name of the bucket which needs to be checked for availablity
 * @returns  Boolean value based upon existence of the bucket on google cloud storage
    This is the helper function which is checking weather the bucket with given name is available or not on google cloud storage
 */
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
/**
 * This is the helper method to create a bucket on Google cloud storage
 * @param {*} name The name of the bucket which needs to be created if not available
 * @returns Boolean value based upon creation job of the bucket on google cloud storage
 */
const createBucket = async (name = "") => {
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
/**
 * This is the Helper function to check existence of the File in a bucket on Google Cloud Storage
 * @param {*} bucketName The name of google storage bucket to be used
 * @param {*} fileName The name of file of that bucket to be checked for existence
 * @returns Boolean value based upon existence of the file in that particular bucket
 */
const checkFileExistence = async (bucketName = "", fileName = "") => {
    const [files] = await storage.bucket(bucketName).getFiles();
    return files.findIndex(file => file.name === fileName) >= 0 ? true : false;;
}

/**
 * This is the Helper Function to Create a File in Bucket
 * @param {*} bucketName The name of google storage bucketin which file is needed to be created
 * @param {*} fileName The name of file of that bucket to be created
 * @param {*} content The content of the file to be written
 * @returns Boolean value based upon creation of the file in that particular bucket
 */
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
/**
 * This is the Helper Function to update content in the file
 * @param {*} bucketName The name of google storage bucket in which file is needed to be updated
 * @param {*} fileName The name of file of that bucket to be updated
 * @param {*} content The content of the file to be written
 * @returns Boolean value based upon updation of the file in that particular bucket
 */
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
/**
 * This is the Helper Function to read file on Google Cloud Storage
 * @param {*} bucketName The name of google storage bucket in which file is needed to be Read
 * @param {*} fileName The name of file of that bucket to be updated
 * @returns  null if file is empty, sliced string if file contains data and false if error
 */
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
/**
 * This is the Helper Function which will perform task of calculating difference in last deploy and current deploy time of Google Cloud Function
 * @param {*} isBucket  boolean value of if bucket exist on Google Cloud Storage
 * @param {*} bucketName The name of google storage bucket  to be used
 * @param {*} fileName The name of file in that bucket to be used 
 * @param {*} content  The content to add in thatr file
 * @param {*} context context of the executed Google Cloud Function
 */
const bucketCrud = async (isBucket = false, bucketName = "", fileName = "", content = "", context="") => {
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
csvQuery();
/**
 * This is the main entery point of the application from where Google Cloud Function Starts its execution
 * @param {*} event  event of the executed Google Cloud Function
 * @param {*} context context of the executed Google Cloud Function
 */
// exports.helloPubSub = async (event, context) => {
//     const bucketName = "bucket-mikmak-data-project-hello-world";
//     const fileName = "logs.txt";
//     const content = `lastTimestamp=${context.timestamp}`
//     const message = event.data
//         ? Buffer.from(event.data, 'base64').toString()
//         : 'Hello, World';
//     console.log("message is---", message);
//     const isBucket = await checkBucketExistence(bucketName);
//     bucketCrud(isBucket, bucketName, fileName, content, context);
  
// };
