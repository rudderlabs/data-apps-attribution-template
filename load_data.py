"""
Data needs to be loaded from wh with the given input credentials and input features. This has helper functions to load the data    
This will be used in both feature_processing and predict notebooks to load the data from warehouse. 
#ToDo:
Currently using sqlalchemy, but goingforward it will use wht
"""

from utils import SnowflakeConnector
from typing import List, Dict, Union, Tuple, Optional
import pandas as pd
import logging

class SnowflakeDataIO:
    def __init__(self, snowflake_creds: dict, query_table_name: str, timestamp_column: str) -> None:
        self.snowflake_creds = snowflake_creds
        self.query_table_name = query_table_name
        self.timestamp_column = timestamp_column

    def fetch_data_from_wh(self, query: str) -> pd.DataFrame:
        snowflake = SnowflakeConnector(self.snowflake_creds)
        df = snowflake.run_query(query) 
        snowflake.close()
        return df
    
    def __get_timestamp_where_condition(self, min_date: str) -> Optional[str]:
        if min_date:
            logging.debug("Minimum start date is given, so it is used to filter the data")
            timestamp_condition = f"{self.timestamp_column} >= '{min_date}'"
        else:
            logging.debug("Minimum start date is not defined. So the query runs on all dataset")
            timestamp_condition = None
        return timestamp_condition
    
    def generate_query(self, min_date: Optional[str] = None) -> str:
        """Generates query to fetch latest data from warehouse feature store table. 

        Args:
            min_date (Optional[str], optional): . Defaults to None.

        Returns:
            str: Query string to fetch latest data from warehouse touch data table
        """
        timestamp_condition = self.__get_timestamp_where_condition(min_date)

        if timestamp_condition:
            query = (f"select * from {self.query_table_name} where {timestamp_condition} order by {self.timestamp_column} desc")
        else:
            query = (f"select * from {self.query_table_name} order by {self.timestamp_column} desc")

        return query

    @staticmethod
    def write_to_snowflake_table(df: pd.DataFrame, table_name: str, snowflake_creds: Dict, if_exists: str = "append") -> None:
        """Writes dataframe to warehouse feature store table.
        
        Args:
            df (pd.DataFrame): Dataframe to be written to warehouse feature store table
            table_name (str): Feature store table name
            snowflake_creds (Dict): Snowflake credentials
            if_exists (str, optional): {"append", "replace", "fail"} Defaults to "append".
                fail: If the table already exists, the write fails.
                replace: If the table already exists, the table is dropped and the write is executed.
                append: If the table already exists, the write is executed with new rows appended to existing table
        """
        snowflake_connector = SnowflakeConnector(snowflake_creds)
        df.to_sql(table_name, snowflake_connector.engine, if_exists=if_exists, index=False)
        snowflake_connector.close()
    
def pipe(table_name: str, 
         snowflake_creds: dict, 
         min_date: str,
         timestamp_column: str,
         debug: bool = False,
    ) -> pd.DataFrame:
    """Combine all the above functions to return final processed data

    Args:
        table_name (str): Name of the feature table in WH
        snowflake_creds (dict): Dict with snowflake connector details
        min_date (str): All the data after this date should be consider for analysis
        timestamp_column: Column name where timestamp is stored in the table
        debug (bool, optional): If true, it prints additional messages helpful to debug. Defaults to False

    Returns:
        pd.DataFrame: Final input on which transformations pipeline can be run
    """

    connector = SnowflakeConnector(snowflake_creds)
    sf_connector = SnowflakeDataIO(snowflake_creds=snowflake_creds,
                                   query_table_name=table_name,
                                   timestamp_column=timestamp_column)
    query_string = sf_connector.generate_query(min_date)
    if debug:
        print(f"Query generated: {query_string}")
    input_data = sf_connector.fetch_data_from_wh(query_string)
    print(f"Total no:of datapoints: {len(input_data)}")
    return input_data
