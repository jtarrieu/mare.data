import mariadb # librairie pour se connecter à mariadb
import sys
import var # import variable configured into var.py
import pandas as pd
import random
import os
import json
from copy import deepcopy

class MariadbConnector:
    """
        Class that implement a connector for Mariadb
        Query function that open and close cursor
        Retrieve databases, tables and columns on the server
    """

    def __init__(self) -> None:
        self.dict_full_structure = self._get_columns_in_table()
        self.dict_table_in_database = self._get_table_in_database()

    def _connect_to_mariadb(self):
        """
            Function that instantiate connection to MariaDB
            Return the connector that make queries possible
        """
        try:
            conn = mariadb.connect(
                                user=var.user,
                                password=var.password,
                                host=var.host,
                                port=var.port,
            )
        except mariadb.Error as e :
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        return conn.cursor()

    def query_mariadb(self, query):
        """
            Create the cursor and send query to mariaDB and automatically close the cursor
            Return all the cursor
        """
        cursor = self._connect_to_mariadb()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


    def _get_databases_list(self):
        """
            Return a list of all databases on the server
        """
        database_list = list()
        unwanted_database = ['information_schema','mysql','sys','performance_schema']
        results = self.query_mariadb(query="SHOW DATABASES;")
        for database in results:
            if database not in unwanted_database:
                database_list.append(database[0])
        return database_list

    def _get_table_in_database(self) -> dict:
        """
            Return a dictionnary with database as key and a dictionnary of their table as value
            {
                database_1 : {
                                table_1: {},
                                table_2: {}, 
                                ...
                            },
                database_2 : {
                                table_1: {},
                                table_2: {}, 
                                ...
                            },
                ...
            }
        """
        dict_table_database = dict()
        for database in self._get_databases_list():
            dict_table_database[database] = dict()
            results = self.query_mariadb(query=f"SHOW TABLES FROM {database}")
            results = [value[0] for value in results] # keep only the name of the table
            for table in results:
                dict_table_database[database][table] = dict()
        return dict_table_database
    
    def _get_columns_in_table(self) -> dict:
        """
            Return the full structure of databases on the server by retrieving columns in table
            {
                database_1 : {
                                table_1: {
                                        column_1 : [],
                                        column_2 : [],
                                        ...
                                },
                                table_2: {
                                        ...
                                }, 
                                ...
                            },
                ...
            }
        """
        dict_table_in_database = deepcopy(self.dict_table_in_database)
        for database in dict_table_in_database.keys():
            for table in dict_table_in_database[database]:
                results = self.query_mariadb(query=f"SHOW COLUMNS FROM {database}.{table}")
                columns = [value[0] for value in results]
                for i in columns:
                    dict_table_in_database[database][table][columns] = []
        return dict_table_in_database
    
class ExtractData(MariadbConnector):
    """
        Implement the extraction of data from all database on the server 
    """
    def __init__(self) -> None:
        super().__init__()
        self.database_to_join = ['mouleconnected']
        self.database_no_join = ['meteo1']