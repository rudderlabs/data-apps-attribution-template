import os
import yaml
import logging
import sagemaker
import boto3

import pandas as pd

from sqlalchemy import create_engine
from pathlib import Path
from sklearn.base import BaseEstimator, TransformerMixin
from typing import Optional, List, Tuple
from glob import glob

try:
    import StringIO
except:
    from io import StringIO
    

def create_logger(log_file_name: str='log.log', log_level: int=logging.INFO) -> logging.Logger:
    """
    Get a logger object.

    :param log_file_name: The name of the log file.
    :param log_level: The log level.
    :return: The logger object.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # create a file handler
    handler = logging.FileHandler(log_file_name)
    handler.setLevel(log_level)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    return logger


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    s3_bucket = s3_path.split("://")[1].split("/")[0]
    s3_file_location = "/".join(s3_path.split("://")[1].split("/")[1:])
    return s3_bucket, s3_file_location


def pd_to_csv_s3(df: pd.DataFrame, 
                 s3_bucket_name: str,
                 s3_path: str, 
                 s3_resource, 
                 index: bool=False,
                 header: bool=False) -> None:
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=index,header=header)
    s3_resource.Object(s3_bucket_name, s3_path).put(Body=csv_buffer.getvalue())


def read_csv_from_s3(s3_bucket, 
                     file_path, 
                     boto_session, 
                     header: Optional[int]=None, 
                     index: Optional[str]=None) -> pd.DataFrame:
    s3_client = boto_session.client("s3")
    csv_obj = s3_client.get_object(Bucket=s3_bucket, Key=file_path)
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8")
    return pd.read_csv(StringIO(csv_string), header=header, index_col=index)


def create_s3_resource(aws_cred_file_path: str):
    try:
        sm_role = sagemaker.get_execution_role()
        print("Able to get aws session")
        region = boto3.session.Session().region_name
        return boto3.resource("s3")
    except:
        print("Creating session using aws creds")
        aws_creds = load_config(aws_cred_file_path)["aws"]
        sm_role = aws_creds["roleArn"]
        region = aws_creds["region"]
        boto_session = boto3.Session(
            aws_access_key_id=aws_creds["access_key_id"],
            aws_secret_access_key=aws_creds["access_key_secret"],
            region_name=aws_creds["region"])
        return boto_session.resource("s3")


def get_s3_paths_by_file_name(s3_bucket: str, s3_path_prefix: str, file_name: str, s3_resource) -> List[str]:
    """
    List all folders in the s3 path.
    """
    bucket = s3_resource.Bucket(s3_bucket)
    s3_paths = []
    for obj in bucket.objects.filter(Prefix=s3_path_prefix):
        if file_name in obj.key:
            s3_paths.append(obj.key)
    return s3_paths

def get_latest_folder(path:str, filter_substr:str=None) -> str:
    """Gets latest folder by creation time

    Args:
        path (str): Path to parent folder
        filter_substr (str, optional): If only certain set of folders need to be selected based on a substring match. Defaults to None.

    Returns:
        str: Returns the latest folder path 
    """
    files_and_folders = list(glob(os.path.join(path, "*")))
    if filter_substr:
        files_and_folders = [path for path in files_and_folders if filter_substr in path]
    folder_paths = [ path for path in files_and_folders if os.path.isdir(path)]
    recent_folder_path = max(folder_paths, key=os.path.getctime)
    return recent_folder_path


class SnowflakeConnector:
    def __init__(self, credentials: dict) -> None:
        self.credentials = credentials
        url = f"snowflake://{credentials['user']}:{credentials['password']}@{credentials['account_identifier']}"
        if 'database' in credentials:
            url += f"/{credentials['database']}"
            if 'schema' in credentials:
                url += f"/{credentials['schema']}"
                if 'warehouse' in credentials:
                    url += f"?warehouse={credentials['warehouse']}"
                    if 'role' in credentials:
                        url += f"&role={credentials['role']}"
        self.engine = create_engine(url)
        self.connection = self.engine.connect()

    def run_query(self, query) -> pd.DataFrame:
        query_result = self.connection.execute(query)
        df = pd.DataFrame(query_result.fetchall())
        if len(df) > 0:
            df.columns = query_result.keys()
        else:
            columns = query_result.keys()
            df = pd.DataFrame(columns=columns)

        return df

    def close(self) -> None:
        self.connection.close()
        self.engine.dispose()
    
def load_config(file_path:str) -> dict:
    with open(file_path, "r") as f:
        config = yaml.safe_load(f)
    return config

def get_onehot_encoder_names(onehot_encoder, col_names):
    category_names = []
    for col_id, col in enumerate(col_names):
        for value in onehot_encoder.categories_[col_id]:
            category_names.append(f"{col}_{value}")
    return category_names
    
class NamedColumns(BaseEstimator, TransformerMixin):
    """ 
    Based on the df passed in in fit, filter / reformat the df in transform so the columns match
    Fill any missing columns with default_value
    """

    def __init__(self, default_value = 0):
        self.cols = None
        self.default_value = default_value

    def fit(self, X: pd.DataFrame, y: pd.Series):
        self.cols = X.columns
        return self

    def transform(self, X:pd.DataFrame):
        ret_df = pd.DataFrame(self.default_value, index=X.index, columns=self.cols)
        for col in self.cols:
            if col in X.columns:
                ret_df[col] = X[col]
        return ret_df

if __name__ == "__main__":
    assert parse_s3_path("s3://bucket/location.txt") == ("bucket", "location.txt")
    assert parse_s3_path("s3://bucket") == ("bucket", "")
    assert parse_s3_path("s3://bucket/folder/location.txt") == ("bucket", "folder/location.txt")
    assert parse_s3_path("s3://bucket/folder/") == ("bucket", "folder/")
    assert parse_s3_path("s3://bucket/folder/subfolder/file.txt") == ("bucket", "folder/subfolder/file.txt")