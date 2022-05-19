const moment = require('moment');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const {
  Storage
} = require('@google-cloud/storage');
const {
  BigQuery
} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const storage = new Storage();
/**
 * 
 * @param {*} options 
 * @returns  A promise for batch processed database rows results
 */
const getDBRecords = (options = {}) => {
  return new Promise((resolve) => {
    bigquery.createQueryJob(options).then((data) => {
      const job = data[0];
      console.log(`Job ${job.id} started.`);
      job.getQueryResults().then(res => {
        resolve(res[0]);
      })
    })
  })
}
/**
 * 
 * @param {*} db Databe Name | string
 * @param {*} table  Table Name | tstring
 */
const getColumnNames = async (db = "", table = "") => {
  try {
    const query = `SELECT column_name
    FROM ${db}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '${table}';`;
    const options = {
      query: query,
      // Location must match that of the db(s) referenced in the query.
      location: 'US',
    };
    const [job] = await bigquery.createQueryJob(options);
    let [result] = await job.getQueryResults();
    result = result.map((key, index, arr) => arr[index].column_name);
    return result;
  } catch (err) {
    console.log("Error in getting column names---", err);
  }
}
/**
 * 
 * @param {*} bucketName Name of the bucket to use
 * @param {*} fileName  Name of the file in the bucket
 */
const uploadCSVToGCS = async (bucketName = "", fileName = "") => {
  try {
    await storage.bucket(bucketName).upload("out.csv", {
      destination: fileName
    });
    console.log(`Contents of gs://${bucketName}/${fileName} are updated successfully.`);
  } catch (err) {
    console.log("error while uplaoding out.csv to GCS:-", err);
  }
}
/**
 * This function will do the job of extracting data from database and store in a .csv file on google cloud storage
 * @param {*} bucketName The name of google storage bucket to be used
 * @param {*} fileName The name of file of that bucket to be used
 * @param {*} dataSet  The name of the databse in the project
 * @param {*} table  The name of the table inside that database
 */
const batchProcessRecords = async (bucketName = "", fileName = "", dataSet = "", table = "") => {
  try {
    const columns = await getColumnNames(dataSet, table);
    console.log("columns Names---", columns.join(","));
    const csvWriter = createCsvWriter({
      path: 'out.csv',
      header: columns.map(item => ({ id: item, title: item }))
    });
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
    console.log('Total Records count:', result);
    const batchs = Math.ceil(rowsCount / batchSize);

    for (let i = 0; i < batchs; i++) {
      const query = `SELECT *From ${dataSet}.${table} ORDER BY id LIMIT ${batchSize} OFFSET ${batchSize * i}`;
      const options = {
        query: query,
        location: 'US',
      }
      const resultArr = await getDBRecords(options);
      console.log(`batch ${i + 1} total records--`, resultArr.length);
      await csvWriter.writeRecords(resultArr);
      console.log(`The CSV file's batch ${i + 1} was written in out.csv local file successfully`);
      if (i === batchs - 1) {
        await uploadCSVToGCS(bucketName, fileName);
      }
      console.log("loop end ", i + 1);
    }
  } catch (err) {
    return console.log("Error in catch---", err);
  };


}
/**
 * This is the main function for CSV Task that calls batchProcessRecords() based upon conditional check on if bucket avaialble or not. if the bucket  doesn't exists then it will first create a new bucket and then perform extraction job
 */
const exportDBRecords = async () => {
  try {
    const dataSet = "ampl";
    const table = "mikmak_retailers";
    const bucketName = "bucket-mikmak-data-project";
    const fileName = "mikmak-retailers.csv";
    const isBucket = checkBucketExistence(bucketName);
    if (!isBucket) {
      const bucketAdded = await createBucket(bucketName);
      if (bucketAdded) {
        batchProcessRecords(bucketName, fileName, dataSet, table)
      }
    } else {
      batchProcessRecords(bucketName, fileName, dataSet, table)
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
  }
};
/**
 * This is the Helper Function to update content in the file
 * @param {*} bucketName The name of google storage bucket in which file is needed to be updated
 * @param {*} fileName The name of file of that bucket to be updated
 * @param {*} content The content of the file to be written
 */
const updateFile = async (bucketName = "", fileName = "", content = "") => {
  try {
    await storage.bucket(bucketName).file(fileName).delete();
    await storage.bucket(bucketName).file(fileName).save(content);
    console.log(`${fileName} updated`);

  } catch (error) {
    console.log("error while updating file:-", error);
  }
};
/**
 * This is the Helper Function to read file on Google Cloud Storage
 * @param {*} bucketName The name of google storage bucket in which file is needed to be Read
 * @param {*} fileName The name of file of that bucket to be updated
 * @returns  null if file is empty and sliced string if file contains data 
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
const bucketCrud = async (isBucket = false, bucketName = "", fileName = "", content = "", context = "") => {
  if (!isBucket) {
    const bucketAdded = await createBucket(bucketName);
    if (bucketAdded) {
      await createFile(bucketName, fileName, content);
    }
  } else {
    const isFile = await checkFileExistence(bucketName, fileName);
    if (!isFile) {
      await createFile(bucketName, fileName, content);
    } else {
      const oldContent = await readFile(bucketName, fileName);
      if (oldContent) {
        const diff = moment(oldContent).from(moment(context.timestamp))
        console.log("Time Interval since last Trigger---", diff);
      } else {
        console.log(`This is the first time,when ${fileName} file is created`);
      }
      await updateFile(bucketName, fileName, content);
    }
  }
}
/**
 * This is the main entery point of the application from where Google Cloud Function Starts its execution
 * @param {*} event  event of the executed Google Cloud Function
 * @param {*} context context of the executed Google Cloud Function
 */
helloPubSub = async (event, context) => {
  try {
    const bucketName = "bucket-mikmak-data-project-hello-world";
    const fileName = "logs.txt";
    const content = `lastTimestamp=${context.timestamp}`
    const message = event.data
      ? Buffer.from(event.data, 'base64').toString()
      : 'Hello, World';
    console.log("message is---", message);
    const isBucket = await checkBucketExistence(bucketName);
    await bucketCrud(isBucket, bucketName, fileName, content, context);
    // exportDBRecords();
  } catch (err) {
    console.log("Error in helloPubSub--", err);
  }
};