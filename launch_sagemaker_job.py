"""_summary_
    Launch SageMaker jobs for the ML app from Frontend.
"""
import os
import argparse
import json
import boto3
import yaml
import time
import zipfile

from glob import glob
from typing import List
from pathlib import Path
from collections import defaultdict
from multiprocessing.dummy import Array
from sagemaker.session import Session
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput

def list_all_files_in_directory(directory: str) -> List[str]:
    files_list = []
    for path, _, files in os.walk(directory):
        for name in files:
            files_list.append(os.path.join(path, name))
    return files_list

def download_s3_directory_to_local(s3_resource, s3_bucket_name, s3_path, local_path):
    Path(local_path).mkdir(parents=True, exist_ok=True)
    for obj in s3_resource.Bucket(s3_bucket_name).objects.filter(Prefix=s3_path):
        s3_resource.meta.client.download_file(
            s3_bucket_name, obj.key, os.path.join(local_path, os.path.basename(obj.key)))

config_files = {"multi_touch_attribution": "analysis_config.yaml"}

# BUCKET='ml-usecases-poc'# rudder
BUCKET = 'alvyl-ml-usecases-poc'  # Alvyl
with open("credentials.yaml", "r") as f:
    config = yaml.safe_load(f)

# Aws credentials
BUCKET = config["aws"]["s3Bucket"]
AWS_ACCESS_KEY_ID = config['aws']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['access_key_secret']
AWS_REGION = config['aws']['region']
AWS_ROLE_ARN = config["aws"]["roleArn"]
DATA_FOLDER = "data"

if __name__ == "__main__":
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--id", help="Instance ID for Rudder Client", type=str)
    arg_parser.add_argument('--job', type=str, default="data_prep",
                            help='One of [data_prep, train, predict]')
    arg_parser.add_argument('--instance', type=str, default="local",
                            help="If `local`, job runs locally with docker containers. Else pass a valid aws machine type. ex: ml.t3.xlarge")
    args = arg_parser.parse_args()
    job = args.job
    client_id = args.id
    INSTANCE = args.instance

    if not client_id:
        client_id = str(int(time.time()))
        print("Run ID not provided. Using current time as client ID")

    print(f"Client ID: {client_id}")

    boto_session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION#,
        #aws_session_token=AWS_SESSION_TOKEN
    )
    sagemaker_session = Session(
        boto_session=boto_session, default_bucket=BUCKET)

    try:
        assert job in ["multi_touch_attribution"]
    except AssertionError:
        print(f"Job: {job} not in list data_prep, train, predict")
        raise

    job_suffix = {'multi_touch_attribution': 'MTA'}
    job_name = f'{client_id}-{job_suffix[job]}'

    sklearn_processor = SKLearnProcessor(framework_version='0.20.0',
                                         role=AWS_ROLE_ARN,
                                         instance_type=INSTANCE,
                                         instance_count=1,
                                         base_job_name=job_name,
                                         sagemaker_session=sagemaker_session)
    # Add all dependency files here
    files = ["load_data.py", "utils.py"]

    with zipfile.ZipFile("utils.zip", "w") as zipobj:
        for file in files:
            if file.endswith("*"):
                for f in list_all_files_in_directory(file[:-1]):
                    zipobj.write(f)
            else:
                zipobj.write(file)

    notebook_file = f"{job}.ipynb"
    container_path_main = "/opt/ml/processing"
    utils_path = f"{container_path_main}/input/code/utils"
    requirements_path = f"{container_path_main}/requirements/"
    notebook_path = f"{container_path_main}/notebooks/"
    config_path = f"{container_path_main}/code/config/"
    credentials_path = f"{container_path_main}/config"
    utils_extract_to_path = f"{container_path_main}/input/code"
    # output_path = f"{container_path_main}/output/"
    output_path = f"{utils_extract_to_path}/data"

    params = {
        "run_id": client_id, 
        "folder_utils_path": utils_extract_to_path,
        "local_output_path": output_path
    }

    sklearn_processor.run(code='run_notebook_wrapper.py',
                          inputs=[ProcessingInput(
                              source=notebook_file,
                              destination=notebook_path
                          ),
                              ProcessingInput(source="utils.zip",
                                              destination=utils_path),
                              ProcessingInput(
                              source='requirements.txt',
                              destination=requirements_path
                          ),
                              ProcessingInput(
                              source=os.path.join("config",config_files[job]),
                              destination=config_path
                          ),
                              ProcessingInput(
                              source='credentials.yaml',
                              destination=credentials_path
                          )],
                          outputs=[ProcessingOutput(source=output_path)],
                          arguments=['--notebooks_path', f"{notebook_path}/{notebook_file}",
                                     '--requirements_path', f"{requirements_path}/requirements.txt",
                                     '--output_path', output_path,
                                     '--utils_path', utils_path,
                                     '--utils_extract_to', utils_extract_to_path,
                                     '--nb_parameters', json.dumps(params)])

    if INSTANCE != "local":
        # Downloading model output files into local
        download_s3_directory_to_local(
            boto_session.resource('s3'), 
            BUCKET, 
            f"{sklearn_processor.latest_job.job_name}/output/output-1/{client_id}/{job}_", 
            f"{DATA_FOLDER}/{client_id}/{job}_"
        )

        # Downloading notebook output file as a html report
        download_s3_directory_to_local(
            boto_session.resource('s3'), 
            BUCKET, 
            f"{sklearn_processor.latest_job.job_name}/output/output-1/{job}_output.html",
            f"{DATA_FOLDER}/{client_id}/{job}_"
        )
    else:
        print("Processor ran locally. Download files from docker container to data/ before moving ahead to the next step.")
        """ 
        Can be done by doing following steps from console:
        > docker container ls -a
        This gives the list of containers. Get the latest container id. 
        Then copy the exact file and use the id as follows:
        > docker cp 1a28de8ebcf5://opt/ml/processing/output/filename .      # This will download the output folder contents to the current directory.
        If this doesnt work, try starting the container and then copy the file
        > docker container start <container_id>
        > docker cp <container_id>://opt/ml/processing/output/filename . 
        """
