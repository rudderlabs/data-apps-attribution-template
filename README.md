# data-apps-mta

## What it is:

In this notebook (`multi_touch_attribution.ipynb`), we calculate multi-touch attribution values for a given set of channels using following methods:
1. Shapley values - considering only positive conversions
2. Markov chain values 
3. First touch based
4. Last touch based 

* Shapley values code is implemented based on the logic presented in this [paper](https://arxiv.org/pdf/1804.05327.pdf)
* Markov chain values are based on the following [whitepaper](https://www.channelattribution.net/pdf/Whitepaper.pdf)

## Prerequisites before building the model:

1. Your event data is setup using RudderStack event stream to your warehouse 
2. You have an aws account with a role that has [`AmazonSagemakerFullAccess`](https://docs.aws.amazon.com/sagemaker/latest/dg/security-iam-awsmanpol.html#security-iam-awsmanpol-AmazonSageMakerFullAccess) policy, and [write access](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html) to an s3 bucket. These details need to be updated in `credentials_template.yaml`. You should also add your aws [access key id/access key secret](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html) and an s3 bucket where the role has write access. 
3. You built a user journey table (or view) in your warehouse. The table should have following columns (it can have other columns too, but they will be ignored):
```
* primary_key_column -> user_id, domain etc. The main entity key
* events_column_name -> touch points. Event_type, campaign_name, page_name etc
* timestamp_column_name -> When the touch has occured

``` 
4. Your warehouse credentials, the user journey table name, and the table where predictions should be written to are updated in the `credentials_template.yaml` file. The file should then be renamed to `credentials.yaml`. Currently, only Snowflake and Redshift are supported.
5. In the `config/analysis_config.yaml`, the data block needs to be updated with the column names from warehouse.
6. Install [anaconda](https://www.anaconda.com/products/distribution)
7. [Optional] Install [docker](https://docs.docker.com/engine/install/). This is required only if you want to run the app locally and not as sagemaker processing job.

## Running multi touch attribution analysis

Run the following command from command line:

> `sh run_analysis.sh ml.t3.xlarge <job_id>

The `<job_id>` accepts any number or string. Recommended values are current timestamp in epoch for job_id and an integer value 0/1 etc for train_id `ml.t3.xlarge` can be replaced by any valid [aws machine type](https://aws.amazon.com/sagemaker/pricing/). If the data size is large, you would need a larger machine.

Once the job is complete, following files get generated under the data folder:
```
data
    <job_id>
      multi_touch_attribution
        ..
        ..
        multi_touch_attribution.html
```
Along with this, the attribution scores are also written in the warehouse. 

## Scheduling the analysis:

If you don't need to schedule the analysis at a set cadence, this section can be skipped. We use aws Lambda and EC2 for scheduling the analysis. 
1. Set up EC2
  1.1 Create an EC2 instance and copy all the files in this directory to the instance. Follow directions from below
  1.2 Copy your aws credentials file (.aws/credentials) to the ec2 instance
2. Create a lambda function that starts the ec2 instance, by copying the code in `lambda_start_ec2.py`. The lambda function should have a role that can start and stop ec2 instances.
3. Create an [event bridge rule](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html) that runs on a schedule and link the lambda function. This launches the ec2 instance at a given schedule every time. The schedule can be configurable based on whatever prediction schedule is desired.
4. Start the ec2 instance and ssh into it (ssh -i <pem_file.pem> ec2-user:<ec2-public-dns>). Setup following in cron:
  ```
  #crontab -e
  @reboot /home/ec2-user/predict_on_ec2.sh
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

> `sh run_analysis.sh ml.t3.xlarge <job_id>`



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


## Change Log (14 Jul 2022):

1. Take top n touch points, and group rest all as `others`
2. Make each model independent, so if one fails, it doesn't need to impact others
3. Add First touch to the results
4. Add both normalized and denormalized distributions
5. Input column name would require only three columns (the table _can_ have extra columns too and template can be modified to use them, but base template ignores these):

```
* primary_key_column -> user_id, domain etc. The main entity key
* events_column_name -> touch points. Event_type, campaign_name, page_name etc
* timestamp_column_name -> When the touch has occured

```
6. An extra input parameter, which tells what the conversion event is called as. Inside the notebook, all the events that occur after the first occurence of this event are ignored. This can be done within the warehouse table too, to reduce data and compute costs.
