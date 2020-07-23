import pandas as pd
import pyodbc
import logger
import numpy as np
import os
import yaml
import sys


class DBConfigLoader:
    def __init__(self, config_path):
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
        return self.db_config


class DBExecutor:
    def __init__(self, config_path="../db_config.yml"):
        self.conn = None
        self.db_config_loader = DBConfigLoader(config_path)
        self.log = logger.get_logger()

    def create_connection(self):
        db_config = self.db_config_loader.get_config()
        if not self.conn:
            self.log.info("Connecting to db ")
            try:
                self.conn = pyodbc.connect(f"""DRIVER={db_config['driver']};
                                              SERVER={db_config['server']};
                                              DATABASE={db_config['database']};
                                              UID={db_config['uid']};PWD={db_config['pwd']}""")
                self.log.info("Connection to db created")
            except ConnectionError:
                self.log.error(f"Connection to database was not created: {ConnectionError}")
        else:
            self.log.info("Connection already created!")

    def close_connection(self):
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
        if self.conn:
            self.log.info("Executing query ")
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                self.conn.commit()
                self.log.info("Query was successfully executed and committed")
            except Exception as e:
                self.log.error(e)
                self.conn.rollback()
        else:
            raise Exception("No active connection to the DB. Make sure it was established.")

    def execute_pandas_query(self, query):
        if self.conn:
            self.log.info("Executing pandas query ")
            try:
                df = pd.read_sql(query, self.conn).replace(np.nan, 'NULL')
                return df
            except ConnectionError:
                self.log.error(ConnectionError, " No active connection to the DB")
        else:
            raise Exception("No active connection to the DB. Make sure it was established.")