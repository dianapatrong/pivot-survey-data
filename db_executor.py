import pandas as pd
import pyodbc
import logger
import numpy as np
import os


class DBExecutor:

    def __new__(cls, *args, **kwargs):
        """ Defining this class as a singleton to avoid increase concurrency on the impala warehouse"""
        if not hasattr(cls, 'instance'):
            cls.instance = super(DBExecutor, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.driver = 'driver'
        self.server = 'server'
        self.database = 'database'
        self.uid = 'userid'
        self.pwd = 'password'
        self.conn = None

    def create_connection(self):
        if not self.conn:
            print("Connecting to db ")
            try:
                self.conn = pyodbc.connect(f"""DRIVER={self.driver};
                                      SERVER={self.server};
                                      DATABASE={self.database};
                                      UID={self.uid};PWD={self.pwd}""")
                print("Connection to db created")
            except ConnectionError:
                print("connection to db failed")
        else:
            print("-- Connection already created! --")

    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                print("Connections closed")
            except ConnectionError:
                print(ConnectionError)
        else:
            print("No current connections to close")

    def execute_query(self, query):
        if self.conn:
            print("executing query")
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                self.conn.commit()
                print("query executed and commited successfully")
            except Exception as e:
                print(e)
                self.conn.rollback()

    def execute_pandas_query(self, query):
        print(self.conn)
        if self.conn:
            print("executing pandas query ")
            try:
                df = pd.read_sql(query, self.conn).replace(np.nan, 'NULL')
                return df
            except ConnectionError:
                print(ConnectionError, " No active connection to the DB")
