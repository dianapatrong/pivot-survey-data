import pandas as pd
import pyodbc
import logger
import numpy as np
import yaml
import sys


class DBConfigLoader:

    def __init__(self, config_path):
        """
        Initializes the db configuration from a given path
        :param config_path: Path to the db configuration in yaml format
        """
        self.log = logger.get_logger()
        try:
            with open(config_path, "r") as config_file:
                self.db_config = yaml.load(config_file, Loader=yaml.FullLoader)
        except FileNotFoundError:
            self.log.error("Configuration file does not exist")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.log.error(f"There was an error in the yaml file: {e}")
            sys.exit(1)

    def get_config(self):
        """
        Gets the db configuration loaded
        :return: the db configuration loaded into a dictionary
        """
        return self.db_config


class DBExecutor:

    def __init__(self, config_path="db_config.yml"):
        self.conn = None
        self.db_config_loader = DBConfigLoader(config_path)
        self.log = logger.get_logger()

    def create_connection(self):
        """
        Creates a connection to the db and checks if it exists one already
        """
        db_config = self.db_config_loader.get_config()
        if not self.conn:
            self.log.info("Connecting to db ")
            try:
                self.conn = pyodbc.connect(f"""
                                            DRIVER={db_config['driver']};
                                            SERVER={db_config['server']};
                                            DATABASE={db_config['database']};
                                            UID={db_config['uid']};
                                            PWD={db_config['pwd']}""")
                self.log.info("Connection to db created")
            except pyodbc.Error as e:
                raise Exception(f"Connection to database was not created: {e}")
        else:
            self.log.info("Connection already created!")

    def close_connection(self):
        """
        Closes the connection to the db
        :return: None
        """
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                self.log.info("Connections closed")
            except ConnectionError:
                self.log.error(ConnectionError)
        else:
            self.log.info("No current connections to close")

    def execute_query(self, query):
        """
        Executes a given query
        :param query: query to be executed against the db
        :return: None
        """
        if self.conn:
            self.log.info("Executing query ")
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                self.conn.commit()
                self.log.info("Query was successfully executed and committed")
            except Exception as e:
                self.log.error(f"There was an error: {e}")
                self.conn.rollback()
                raise Exception(e)
        else:
            raise Exception("No active connection to the DB. Make sure it was established.")

    def execute_pandas_query(self, query):
        """
        Returns the result set of a given query in the form of a pandas Dataframe, it replaces NAN values with NULL
        :param query: query to be executed against the db
        :return: pandas Dataframe with the result of the query
        """
        if self.conn:
            self.log.info("Executing pandas query ")
            try:
                df = pd.read_sql(query, self.conn).replace(np.nan, 'NULL')
                return df
            except Exception as e:
                self.log.error(f"There was an error: {e}")
        else:
            raise Exception("No active connection to the DB. Make sure it was established.")
