import os
import pandas as pd

from sqlalchemy.engine.url import URL
from sqlalchemy import orm as sa_orm
from sqlalchemy import create_engine

import redshift_connector
import pandas_redshift as pr

class ConnectorBase:
    def __init__(self, creds: dict, db_config: dict, aws_config: dict):
        self.creds = creds
        self.db_config = db_config
        self.aws_config = aws_config
        self.engine = None
        self.connection = None

    def run_query(self, query: str):
        query_result = self.connection.execute(query)
        df = pd.DataFrame(query_result.fetchall())
        if len(df) > 0:
            df.columns = query_result.keys()
        else:
            columns = query_result.keys()
            df = pd.DataFrame(columns=columns)
        return df

    def write_to_table(self, df: pd.DataFrame, table_name: str, schema: str = None, if_exists: str = "append"):
        raise NotImplementedError()

    def __del__(self):
        self.connection.close()
        if self.engine is not None:
            self.engine.dispose()

class SnowflakeConnector(ConnectorBase):
    def __init__(self, creds: dict, db_config:dict, aws_config:dict) -> None:
        super().__init__(creds, db_config, aws_config)
        url = f"snowflake://{creds['user']}:{creds['password']}@{creds['account_identifier']}"
        if 'database' in db_config:
            url += f"/{db_config['database']}"
            if 'schema' in db_config:
                url += f"/{db_config['schema']}"
                if 'warehouse' in creds:
                    url += f"?warehouse={creds['warehouse']}"
                    if 'role' in creds:
                        url += f"&role={creds['role']}"
        self.engine = create_engine(url)
        self.connection = self.engine.connect()

    def write_to_table(self, df: pd.DataFrame, table_name: str, schema: str = None, if_exists: str = "append"):
        table_name, schema = table_name.split('.') if '.' in table_name else (table_name, schema)
        print("Writing to table: {}.{}".format(schema, table_name))
        df.to_sql(table_name, self.engine, schema=schema, if_exists=if_exists, index=False)

class RedShiftConnector(ConnectorBase):
    def __init__(self, creds: dict, db_config:dict, aws_config:dict) -> None:
        super().__init__(creds, db_config, aws_config)

        #url = URL.create(
        #    drivername='redshift+redshift_connector',
        #    host=creds['host'],
        #    port=creds['port'],
        #    database=db_config['database'],
        #    username=creds['user'],
        #    password=creds['password'],
        #)

        #self.engine = create_engine(url)
        self.engine = create_engine(f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{db_config['database']}")

        Session = sa_orm.sessionmaker()
        Session.configure(bind=self.engine)
        self.connection = Session()

    def write_to_table(self, df: pd.DataFrame, table_name: str, schema: str = None, if_exists: str = "append"):
        table_name, schema = table_name.split('.') if '.' in table_name else (table_name, schema)

        pr.connect_to_redshift(
            dbname = self.db_config["database"],
            host = self.creds["host"],
            port = self.creds["port"],
            user = self.creds["user"],
            password = self.creds["password"]
        )

        s3_bucket = self.creds.get("s3Bucket", None)
        s3_bucket = s3_bucket if s3_bucket is not None else self.aws_config["s3Bucket"]

        s3_sub_dir = self.creds.get("s3SubDirectory", None)
        s3_sub_dir = s3_sub_dir if s3_sub_dir is not None else self.aws_config["s3SubDirectory"]

        pr.connect_to_s3(
            aws_access_key_id = self.aws_config["access_key_id"],
            aws_secret_access_key = self.aws_config["access_key_secret"],
            bucket = s3_bucket,
            subdirectory = s3_sub_dir
            # As of release 1.1.1 you are able to specify an aws_session_token (if necessary):
            # aws_session_token = <aws_session_token>
        )
        
        # Write the DataFrame to S3 and then to redshift
        pr.pandas_to_redshift(
            data_frame = df,
            redshift_table_name = f"{schema}.{table_name}",
            append = if_exists == "append",
        )

connection_map = { "snowflake" : SnowflakeConnector, "redshift" : RedShiftConnector }

def Connector(config: dict, aws_config: dict = None) -> ConnectorBase:
    """
    Creates a connector object based on the config provided.

    :param config: A dictionary containing the connection configuration.
    :type config: dict
    :return: A connector object.
    :rtype: ConnectorBase
    :raises Exception: 

    """
    name = config.get("name").lower() if "name" in config else ""
    creds = config.get(name) if name in config else None
    if creds is None:
        raise Exception("No credentials found for {}".format(name))
    
    connector = connection_map.get(name, None)
    if connector is None:
        raise Exception(f"Connector {name} not found")

    connector = connector(creds, config, aws_config)
    return connector
