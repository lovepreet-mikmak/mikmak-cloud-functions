const ObjectsToCsv = require('objects-to-csv');
const moment = require('moment')
const {
    Storage
} = require('@google-cloud/storage');
const {
    BigQuery
} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const storage = new Storage({ keyFilename: "storage_key.json" });
const brands = [
    {
        brand_id: "8d062aa4-cbb2-005e-9a68-7a7e7cb689ef",
        export_cloud_storage: "liveramp_export_test_bucket_1"
    },
    {
        brand_id: "4f76bae7-8ff6-1fc6-f874-2272854dfc45",
        export_cloud_storage: "liveramp_export_bucket_2"
    }
]
const DB = "ampl_dev";
const TABLE = "liveramp_export_test_table";
/**
 * 
 * @param {*} options 
 * @returns  A promise for batch processed database rows results
 */
const getDBRecords = (options = {}) => {
    try {
        return new Promise((resolve) => {
            bigquery.createQueryJob(options).then((data) => {
                const job = data[0];
                console.log(`Job ${job.id} started.`);
                job.getQueryResults().then(res => {
                    resolve(res[0]);
                })
            })
        })
    } catch (e) { console.log("error in getDBRecords---", e) }
}

/**
 * This function will do the job of extracting data from database and store in a .csv file on google cloud storage
 * @param {*} bucketName The name of google storage bucket to be used
 * @param {*} fileName The name of file of that bucket to be used
 * @param {*} dataSet  The name of the databse in the project
 * @param {*} table  The name of the table inside that database
 * @param {*} brand_id Brand id to be used in where clause of query
 */
const batchProcessRecords = async (bucketName = "", fileName = "", dataSet = "", table = "", brand_id = "") => {
    try {
        const batchSize = 100;
        const rowCountQuery = `SELECT COUNT(*) From ${dataSet}.${table}`;
        const rowCountQueryOptions = {
            query: rowCountQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: 'US',
        };
        const [job] = await bigquery.createQueryJob(rowCountQueryOptions);
        const [result] = await job.getQueryResults();
        const rowsCount = result && result.length ? result[0].f0_ : 0;
        console.log(`Total Records count for brand id = ${brand_id}: ${rowsCount}`);
        const batches = Math.ceil(rowsCount / batchSize);
        for (let i = 0; i < batches; i++) {
            const query = `SELECT * From ${dataSet}.${table} WHERE brand_id = '${brand_id}' LIMIT ${batchSize} OFFSET ${batchSize * i}`;
            const options = {
                query: query,
                location: 'US',
            }
            console.log("query---", query);
            const resultArr = await getDBRecords(options);
            console.log(`batch ${i + 1} Total records for brand id: ${brand_id}--`, resultArr.length);
            const arr = resultArr.map((item) => {
                Object.keys(item).map(key => typeof item[key] === "boolean" ? (item[key] ? item[key] = "true" : item[key] = "false") : null);
                return item;
            })
            const csv = new ObjectsToCsv(arr);
            const makeHeaders = i === 0 ? true : false;
            const csvString = await csv.toString(makeHeaders);
            await uploaadCSVFromMemory(bucketName, fileName, csvString);
            console.log(`The CSV file's batch ${i + 1} was written successfully`);
            if (i === batches - 1) {
                console.log(`Total CSV Writting Process of file:-${fileName} for brand id: ${brand_id} completed successfully on bucket: ${bucketName}`);
            }
        }
    } catch (err) {
        return console.log("Error in catch---", err);
    };
}
/**
 * This is the main function for CSV Task that calls batchProcessRecords() based upon conditional check on if bucket avaialble or not. if the bucket doesn't exists then it will first create a new bucket and then perform extraction job
 * 
 * @param {*} bucketName | The name of the bucket which needs to be used.
 */
const exportDBRecords = async (bucketName = "", brand_id = "") => {
    try {
        const fileName = `mikmak-retailers_${moment().toISOString()}.csv`;
        const isBucket = await checkBucketExistence(bucketName);
        if (!isBucket) {
            const bucketAdded = await createBucket(bucketName);
            if (bucketAdded) {
                await batchProcessRecords(bucketName, fileName, DB, TABLE, brand_id)
            }
        } else {
            await batchProcessRecords(bucketName, fileName, DB, TABLE, brand_id)
        }
    } catch (err) {
        console.log("Error in catch---", err);
    };

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
    } catch (error) {
        console.log("error occured during fetching buckets list:-", error);
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
    } catch (error) {
        console.log(`error occured during creating bucket name = ${name} and error is:-`, error);
    }
};
/**
 * This is the Helper function to check existence of the File in a bucket on Google Cloud Storage
 * @param {*} bucketName The name of google storage bucket to be used
 * @param {*} fileName The name of file of that bucket to be checked for existence
 * @returns Boolean value based upon existence of the file in that particular bucket
 */
const checkFileExistence = async (bucketName = "", fileName = "") => {
    try {
        const [files] = await storage.bucket(bucketName).getFiles();
        return files.findIndex(file => file.name === fileName) >= 0 ? true : false;
    } catch (error) {
        console.log("error occured during checking File Existence:-", error);
    }
}
const uploaadCSVFromMemory = async (bucketName = "", fileName = "", content = "") => {
    try {
        const isFile = await checkFileExistence(bucketName, fileName);
        if (!isFile) {
            await storage.bucket(bucketName).file(fileName).save(content);
        } else {
            await storage.bucket(bucketName).file('tempFile').save(content);
            await storage.bucket(bucketName).combine([fileName, 'tempFile'], fileName);
            await storage.bucket(bucketName).file('tempFile').delete();
        }
    }
    catch (error) {
        console.log("error while uploading csv from memory :-", error);
    }
}

/**
 * This is the main entery point of the application from where Google Cloud Function Starts its execution
 * @param {*} event  event of the executed Google Cloud Function
 * @param {*} context context of the executed Google Cloud Function
 */
exports.main = async (event, context) => {
    try {
        const message = event.data
          ? Buffer.from(event.data, 'base64').toString()
          : 'Hello, World';
        console.log("message is---", message);
        // const fileName = "logs.txt";
        // const content = `lastTimestamp=${context.timestamp}`

        // const isBucket = await checkBucketExistence(bucketName);
        // await bucketCrud(isBucket, bucketName, fileName, content, context);
        brands.map(async (brand) => {
            const bucketName = brand.export_cloud_storage;
            await exportDBRecords(bucketName, brand.brand_id);
        });
    } catch (err) {
        console.log("Error in main--", err);
    }
};