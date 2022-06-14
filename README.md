# data-apps-mta

## What it is:


## Prerequisites before building the model:

1. Your event data is setup using RudderStack event stream to your warehouse 
2. You have an aws account with a role that has [`AmazonSagemakerFullAccess`](https://docs.aws.amazon.com/sagemaker/latest/dg/security-iam-awsmanpol.html#security-iam-awsmanpol-AmazonSageMakerFullAccess) policy, and [write access](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html) to an s3 bucket. These details need to be updated in `credentials_template.yaml`. You should also add your aws [access key id/access key secret](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) and an s3 bucket where the role has write access. 
3. You built a feature table in your warehouse using either dbt or wht. The current template uses a [dbt project](https://github.com/rudderlabs/dbt-user-touchpoints) from where the feature store table is getting generated in snowflake. 
4. Your warehouse credentials, the feature store table name, and the table where predictions should be written to are updated in the `credentials_template.yaml` file. The file should then be renamed to `credentials.yaml`. Currently, only Snowflake is supported.
5. In the `config/data_prep_config.yaml`, following block needs to be updated with the label column and entity column. 

```
data:
  columns:
    # Label column name
    label: converted
    # Entity (unique key for the final features table formation), can be single or list of column names
    entity: user
```
6. Install [anaconda](https://www.anaconda.com/products/distribution)
7. [Optional] Install [docker](https://docs.docker.com/engine/install/). This is required only if you want to run the app locally and not as sagemaker processing job.

## Running multi touch attribution analysis

Run the following command from command line:

> `sh full_pipeline.sh train ml.t3.xlarge <job_id>

The `<job_id>` accepts any number or string. Recommended values are current timestamp in epoch for job_id and an integer value 0/1 etc for train_id `ml.t3.xlarge` can be replaced by any valid [aws machine type](https://aws.amazon.com/sagemaker/pricing/). If the data size is large, you would need a larger machine.

Once the job is complete, following files get generated under the data folder:
```
data
    job0
      multi_touch_attribution
        ..
        ..
        multi_touch_attribution.html
```

5. Stop the instance. 

With these steps, the predictions are scheduled on an ec2 instance at set frequency. The event bridge triggers lambda function at set frequency. The function then starts the ec2 instance. The cron task then runs the predict_on_ec2 job, which does the predictions, and then shuts off the instance once the prediction is done.

## Setting up on EC-2:
1. [Create an ec2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html), and ssh to the instance
2. Get a recent anaconda package Linux url from its [archive](https://repo.continuum.io/archive/index.html) and download in the ec2 machine by doing the following:
```
  > wget https://repo.anaconda.com/archive/Anaconda3-5.3.1-Linux-x86_64.sh # Can be replaced by any other linux url from the arhive location
  > bash Anaconda3-5.3.1-Linux-x86_64.sh
  > source .bashrc
  > source ~/anaconda3/etc/profile.d/conda.sh
  > export PATH="/home/ec2-user/anaconda3/bin:$PATH"
```
3. Transfer files from local to ec2:
  * Create a [pem file](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html#create-a-key-pair) from aws (my_file.pem)
  * `> scp -i my_file.pem -r * ec2-user@<my-ec2-public-dns>.us-east-2.compute.amazonaws.com:`
4. Run the full pipeline command just as in local

`sh full_pipeline.sh train ml.t3.xlarge <job_id>`

5. If atleast one cycle of train is done, the script can be run in predict mode:

`sh full_pipeline.sh predict ml.t3.xlarge <job_id> `


## Appendix:

**Details of what's happening under the hood:**


**Debug:**

1. First time when running locally, it tries to download the container from aws. It needs authenticating 
2. After multiple local runs, there may be many docker container images stored locally. These need to be cleaned if there was an error that says `No space left on device`. This is a docker error. All the inactive containers can be deleted by `docker container prune`. PS: THIS DELETES ALL THE CONTAINERS, NOT JUST RELATED TO DATA APPS OR THIS SPECIFIC REPOSITORY. DO NOT USE THIS IF YOU USE DOCKER FOR OTHER PURPOSES.
3. In local mode, at end of each step, following steps need to be performed to download data from docker containers:
    > docker container ls -a 
    
    This gives the list of containers. Get the latest container id. 
    
    Then copy the exact file and use the id as follows:
    
    > docker cp <container_id>://opt/ml/processing/output/filename .      # This will download the output folder contents to the current directory.
    
    If this doesnt work, try starting the container and then copy the file
    
    > docker container start <container_id>
    
    > docker cp <container_id>://opt/ml/processing/output/filename . 
