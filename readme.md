# mikmak-cloud-functions
## This Project is a basic example of Integration of Google Cloud Scheduler, Google cloud Scheduler and Google Cloud Functions.

The Main ojbective of this codebase is to create a cloud function that will auto-run on a scheduled time (using Google Cloud schdeuler) to calculate time differnece in the current and last auto triggered execution by storing the last time triggered function's timestamp in a file on a bucket in Google Cloud Storage.

This Repository's Deployment on GCP is made CI/CD compatible with the help of github actions and github workflows. For details on workflow kindly navigate to .github/workflows inside the project repo.

## Steps for the acheiving the above written objective are divided into two broad categories:-
- Steps to Perform on Google Cloud Platform.
- Steps to Perform on Github.

### Steps to Perform on Google Cloud Platform.

- Create a Project in GCP.
- Inside that Project, Create a Service account and enable billing for it.
- The Service account must have these following Roles:- Cloud Functions Admin, Service Account User, Storage Admin and Storage Object Viewer. Make sure Your service account must have these roles and permissions by navigating to IAM settings in GCP.
- Create a Service Account Key and copy to clipboard for later use.
- Copy Project Id to clipboard for later use.
- In GCP Top Navbar, type `Topics` in search bar and Create  a Pub/sub Topic on that page by following given instructions on that page.
- Create a subscription for it explicitly if `Add a default subscription` option was unchecked while topic creation.
- Now Go To Google Cloud Scheduler in GCP to trigger that topic after a set scheduled time.
- Inside Google Cloud Scheduler, Tap on `Create Job`, fill basic details and cron frequency field and click on next button.
- on Next section Select 'Target type' as 'Pub/Sub' and in Topic Form Field select the above created topic from dropdown list. Click on save. 
- Copy the created Topic id from Topic list for later use.
> Note: Now our Scheduler is all set to trigger that Particular topic on the set date and time.

### Steps to Perform on Github.
- Create your new empty Github Repo.
- In browser open this Github Repo https://github.com/lovepreet-mikmak/mikmak-cloud-functions . and copy-paste .gitignore, index.js, .github and package.json file(s)/folder(s) from this to your repo. 
- Inside settings on Your Repo, store the above copied service-account-key, project-id and topic-id as Secrets with names `GCP_SA_KEY` , `GCP_PROJECT_ID` and `GCP_PROJECT_TOPIC_ID` respectively .
- Run `npm install`.
- After making any changes to codebase, Run github actions workflow under actions tabs on github.
- Upon Successfull workflow execution. A Google Cloud function with name `pub_sub_hello_world_example` will be created on GCP and on auto-triggering of this function will create a Google Cloud storage bucket and a file named as "logs.txt"  in that bucket.
- You can change the function name in the .github/workflows/deploy_cloud_functions.yml file.
- Now your Cloud Function will automatically listen and trigger based upon time set for your topic in Google Cloud Scheduler.
