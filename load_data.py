"""
Data needs to be loaded from wh with the given input credentials and input features. This has helper functions to load the data    
This will be used in both feature_processing and predict notebooks to load the data from warehouse. 
#ToDo:
Currently using sqlalchemy, but goingforward it will use wht
"""

from wh_connectors import Connector
from typing import List, Dict, Union, Tuple, Optional
import pandas as pd
import logging
import numpy as np

class DataIO:
    def __init__(self, 
                 config: dict, 
                 feature_store_table: str,
                 label_column: str, 
                 entity_column: str,
                 feature_name_column: str, 
                 timestamp_column: str,
                 numeric_value_column: str,
                 str_value_column: str,) -> None:
        self.config = config
        self.feature_store_table = feature_store_table
        self.label_column= label_column
        self.entity_column = entity_column
        self.feature_name_column = feature_name_column
        self.timestamp_column = timestamp_column
        self.numeric_value_column = numeric_value_column
        self.str_value_column = str_value_column
        pass

    def fetch_data_from_wh(self, query: str) -> pd.DataFrame:
        wh_conn = Connector(self.config)
        df = wh_conn.run_query(query) 
        return df
    
    def __get_timestamp_where_condition(self, timestamp_column: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> Optional[str]:
        if start_time:
            if end_time:
                logging.debug("Feature start date and end date too are given, so they is used to filter the data")
                timestamp_condition = f"{timestamp_column} between '{start_time}' and '{end_time}'"
            else:
                logging.debug("Feature start date is given, so it is used to filter the data")
                timestamp_condition = f"{timestamp_column} >= '{start_time}'"
        elif end_time:
            logging.debug("Feature end date is given, so it is used to filter the data")
            timestamp_condition = f"{timestamp_column} <= '{end_time}'"
        else:
            logging.debug("Neither start time nor end time defined. So the query runs on all dataset")
            timestamp_condition = None
        return timestamp_condition
    
    def generate_query_for_latest_data(self, 
                                       features_start_date: Optional[str] = None, 
                                       features_end_date: Optional[str] = None,
                                       feature_subset: Optional[Union[List[str], Tuple[str], str]]="*",
                                       no_of_timestamps: Optional[int] = 1) -> str:
        """Generates query to fetch latest data from warehouse feature store table. 

        Args:
            features_start_date (Optional[str], optional): . Defaults to None.
            features_end_date (Optional[str], optional): . Defaults to None.
            feature_subset (Optional[Union[List[str], Tuple[str], str]], optional): . Defaults to "*".
            no_of_timestamps (Optional[int], optional): Number of latest timestamps for which we get the features. Defaults to 1, meaning the latest snapshot.

        Returns:
            str: Query string to fetch latest data from warehouse feature store table.
        """
        if isinstance(feature_subset, list) or isinstance(feature_subset, tuple):
            features_and_label_str = "(" + ", ".join(map(lambda feat: "'" + feat + "'", feature_subset + [self.label_column])) + ")"
        else:
            features_and_label_str = None

        
        inner_query = (f"select {self.entity_column}, {self.feature_name_column}, {self.numeric_value_column}, {self.str_value_column}, {self.timestamp_column}, "
                    f"rank() over (partition by {self.entity_column}, {self.feature_name_column} order by {self.timestamp_column} desc) as rnk from {self.feature_store_table}")
        timestamp_condition = self.__get_timestamp_where_condition(self.timestamp_column, features_start_date, features_end_date)

        if features_and_label_str and timestamp_condition:
            inner_query = f"{inner_query} where {timestamp_condition} and {self.feature_name_column} in {features_and_label_str}"
        elif features_and_label_str:
            inner_query = f"{inner_query} where {self.feature_name_column} in {features_and_label_str}"
        elif timestamp_condition:
            inner_query = f"{inner_query} where {timestamp_condition}"
        query = f"select {self.entity_column}, {self.feature_name_column}, {self.numeric_value_column}, {self.str_value_column}, {self.timestamp_column} from ({inner_query}) as t where rnk <= {no_of_timestamps}"
        return query

    def get_materialized_timestamps_sorted_by_latest(self, 
                                                     config: Dict, 
                                                     table_name: str, 
                                                     timestamp_column: str, 
                                                     features_start_date: Optional[str], 
                                                     features_end_date: Optional[str]) -> List[str]:
        """Returns list of materialized timestamps from warehouse feature store table, sorted by latest to oldest.
         Assumes that at a given timestamp, all features are computed."""
        timestamp_condition = self.__get_timestamp_where_condition(timestamp_column, features_start_date, features_end_date)
        query = f"select distinct {timestamp_column} from {table_name} order by 1 desc"
        if timestamp_condition:
            query = f"{query} where {timestamp_condition}"

        df = self.fetch_data_from_wh(config, query)
        return df.values.flatten().tolist()
    
    @staticmethod
    def write_to_wh_table(
        df: pd.DataFrame, 
        config: Dict, 
        table_name: str, 
        schema:str = None, 
        if_exists: str = "append",
        aws_config: dict = None) -> None:
        """Writes dataframe to warehouse feature store table.
        
        Args:
            df (pd.DataFrame): Dataframe to be written to warehouse feature store table
            config (Dict):  credentials
            table_name (str): Feature store table name
            schema (str, optional): Schema name, Defaults to None.
            if_exists (str, optional): {"append", "replace", "fail"} Defaults to "append".
                fail: If the table already exists, the write fails.
                replace: If the table already exists, the table is dropped and the write is executed.
                append: If the table already exists, the write is executed with new rows appended to existing table
        """
        wh_connector = Connector(config, aws_config)
        wh_connector.write_to_table(df, table_name, schema, if_exists)
    
    def get_feature_data_from_wh(self, 
                                 query: str) -> pd.DataFrame:
        """Gets feature data from warehouse using the given query, converts the long form data to wide form data, and returns the feature set as a pandas dataframe

        Args:
            query (str): _description_
        Returns:
            pd.DataFrame: _description_
        """
        df = self.fetch_data_from_wh( query)
        numeric_data = (df
                        .query(f"~{self.numeric_value_column}.isnull()", engine="python")
                        .pivot_table(index=[self.entity_column, self.timestamp_column], columns=self.feature_name_column, values=self.numeric_value_column, fill_value=0))
        non_numeric_data = (df
                            .query(f"~{self.str_value_column}.isnull() and {self.str_value_column}!=''", engine="python")
                            .pivot(index=[self.entity_column, self.timestamp_column], columns=self.feature_name_column, values=self.str_value_column))

        return numeric_data.merge(non_numeric_data, left_index=True, right_index=True, how="left")

def pipe(table_name: str, 
         config: dict, 
         entity_column: str,
         feature_name_column: str,
         timestamp_column: str,
         numeric_value_column: str,
         str_value_column:str, 
         label_column: str,
         features_subset: Union[List[str], str]="*",
         features_start_date: str = None, 
         features_end_date: str = None,
         debug: bool = False,
         no_of_timestamps: int = 1) -> pd.DataFrame:
    """Combine all the above functions to return final processed data

    Args:
        table_name (str): Name of the feature table in WH
        config (dict): Dict with warehouse connector details
        feature_table_columns_list (List[str]): Column names of the table_name (The col_* params below should all come from within this list)
        entity_column (str): Name of the entity column (ex: user, domain etc)
        feature_name_column (str): Name of the feature column (ex: feature, event, event_type etc)
        timestamp_column (str): Name of the timestamp column (ex: timestamp, ts, date)
        numeric_value_column (str): Name of the column with numeric values
        str_value_column (str): Name of the column with str values
        label_column (str): Name of the feature that corresponds to label. This will be a value in the feature_name_column columne
        features_subset (Union[List[str], str], optional): List of features that need to be used for the model. Defaults to all features. 
        features_start_date (str, optional):  Start date for feature processing data collection. Defaults to None.
        features_end_date (str, optional): End date for feature processing data collection. Defaults to None. 
        debug (bool, optional): If true, it prints additional messages helpful to debug. Defaults to False
        no_of_timestamps (int, optional): Number of latest timestamps to be considered for the data in the given date range. Defaults to 1, the most recent data.

    Returns:
        pd.DataFrame: Final input on which transformations pipeline can be run
    """
    wh_connector = DataIO(config=config,
                                   feature_store_table=table_name,
                                   label_column=label_column, 
                                   entity_column=entity_column, 
                                   feature_name_column=feature_name_column, 
                                   timestamp_column=timestamp_column,
                                   numeric_value_column=numeric_value_column,
                                   str_value_column=str_value_column)
    latest_snapshot_query = wh_connector.generate_query_for_latest_data(features_start_date, features_end_date, features_subset, no_of_timestamps=no_of_timestamps)
    if debug:
        print(f"Query generated: {latest_snapshot_query}")
    input_data = wh_connector.get_feature_data_from_wh(latest_snapshot_query)
    print(f"Total no:of datapoints: {len(input_data)}")
    return input_data
