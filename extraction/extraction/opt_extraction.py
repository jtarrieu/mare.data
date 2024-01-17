import mariadb
import sys 
import pandas as pd
import os
import json
from tqdm import tqdm
import argparse
from extraction import var, utils

class MariadbConnector:
    """
        Class that implement a connector for Mariadb
        Query function that open and close cursor
        Retrieve databases, tables and columns on the server
    """

    def __init__(self) -> None:
        """
            dict_structure is a dictionnary that contains the entire structure of Mariadb databases
            The child class ExtractData uses it to automataly retrieve data from those databases
        """
        self.dict_structure = self._get_structure()

    def _connect_to_mariadb(self):
        """
            Instantiate connection to MariaDB
            Return the connector that make queries possible
        """
        try:
            conn = mariadb.connect(
                                user=var.user,
                                password=var.password,
                                host=var.host,
                                port=3306,
            )
        except mariadb.Error as e :
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
        return conn.cursor()

    def query_mariadb(self, query):
        """
            Create cursor and send query to mariaDB and automatally close the cursor
            Return all rows from query's result
        """
        cursor = self._connect_to_mariadb()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


    def _get_data_Njoinbases_list(self):
        """
            Return a list of all databases on the server
        """
        database_list = list()
        unwanted_database = ['information_schema','mysql','sys','performance_schema']
        results = self.query_mariadb(query="SHOW DATABASES;")
        for database in results:
            if database[0] not in unwanted_database: # retrieve all databases on the server that are not default db
                database_list.append(database[0])
        return database_list
       
    def _get_structure(self) -> dict:
        """
            Return a dictionnary of database with dictionnary of their table with list of column as value
            ```python
            {
                database_1 : {
                                table_1: [column_1, column_2, ...],
                                table_2: [], 
                                ...
                            },
                database_2 : {
                                table_1: [],
                                table_2: [], 
                                ...
                            },
                ...
            }
            ```
        """
        structure = dict()
        for database in self._get_data_Njoinbases_list():
            structure[database] = dict()
            query_table = f"SHOW TABLES FROM {database}"
            tables_info = self.query_mariadb(query=query_table)
            tables_name = [value[0] for value in tables_info] # keep only the name of the table
            for table in tables_name :
                query_column = f"SHOW COLUMNS FROM {database}.{table}"
                columns_info = self.query_mariadb(query=query_column)
                columns_name = [value[0] for value in columns_info]
                structure[database][table] = columns_name
        return structure


class ExtractData(MariadbConnector):
    """
        Implement the extraction of data from all databases on the server
    """

    def __init__(self, database_Wjoin: list, database_Njoin: list) -> None:
        """
            Inherit from MariadbConnector
            database_Wjoin : a list of databases' names as string that require their tables to be joined example:
            ```python
            ['mouleconnected', 'another_db_name', ...]
            ```
            database_Njoin : a list of databases' names as string that do not require their tables to be joinned (all tables contains a fullfilled timestamp column)
            ```python
            ['meteo1', 'another_db_name', ...]
            ```
        """
        super().__init__() # allow to access properties and methods from MariaDbConnector init, ie : self.dict_structure
        self.database_Wjoin = database_Wjoin
        self.database_Njoin = database_Njoin

    def _get_data_Njoin(self, document, database, table) -> dict:
        """
            Retrieve data from a specific database and table and store it in the document variable
            document : a dictionnary 
            ```python
            {
                database_name: {
                    table_1: {
                        
                    }
                }
            }
            ```
            database : name of the database that do not require its table to be joined
            table : specific table for which we want data to be retrieve
            
            return a dictionnary with all items 
            ```python
            {
                database_name: {
                    table_1: {
                        item_1: {
                            col_1: value, col_2: value, ...
                        },
                        item_2: {
                            col_1: value, col_2: value, ...
                        },
                        ...
                    }
                }
            }
            ```
        """
        query = f"SELECT * FROM {database}.{table} ORDER BY 'utctimestamp' ASC" # extract all data ordered by timestamp ascending
        results = self.query_mariadb(query=query)
        rowNb = 0
        for rowNb in range(len(results)): # for each row in the table
            item = f'item_{rowNb}' 
            document[database][table][item] = {}
            columnNb = 0
            for col in self.dict_structure[database][table]: # for each column
                        document[database][table][item][col] = results[rowNb][columnNb] # append data to corresponding column for the considered item
                        columnNb+=1 # go to next column
        return document

    def save_as_ndjson(self, batch_size=500000):
        """
            execute both methods that save as ndjson databases content
            batch_size define the size of rows' batches to write the content in ndjson format
        """
        self._save_no_join(batch_size=batch_size)
        self._save_with_join()


    def _save_no_join(self, batch_size):
        """
            Save the content of databases that do not require join operation
        """
        for db in tqdm(self.database_Njoin, desc='Database progress'):
            # create if not exit a folder with named like the database to store table as ndjson file inside
            current_dir = os.getcwd()
            folder_path = current_dir + '/' + db
            os.makedirs(folder_path, exist_ok=True)
            
            # create a dict document to store data, the document content is cleared out each time we switched database
            document = dict()
            document[db] = dict()
            for table in tqdm(self.dict_structure[db].keys(), desc=f'Extracting table from {db}'):
                # create one file per each table
                # file_path = f'{db}/{table}.ndjson'
                file_path = f'{db}/test_{table}.ndjson'
                document[db][table] = dict()
                
                # query database to retrieve all records in the specified location and store it in the document variable
                document = self._get_data_Njoin(document=document, database=db, table=table)
                lenght = len(document[db][table].items())
                items = list(document[db][table].items())
                # browse document and retrieve content in batch of specified size
                batches = [items[i:i+batch_size] for i in range(0,lenght, batch_size)]
                for i, batch in enumerate(batches):
                        # save the batch content in a file
                        with open(file_path, 'a') as f: # 'a'parameter specify append mode, do not erase previous batch content
                            for key, value in batch:
                                entry = {key:value}
                                f.write(json.dumps(entry, default=utils.nan_serializer) + '\n')
                del document[db][table] # free up memory usage when all the content has been saved
    
    def _save_with_join(self):
        """
            Save the content of databases that require their table to be joined
            !! need to implement functions like _mouleconnected_to_dict() and _join_mouleconnected() per each specific database_Wjoin
        """
        for db in self.database_Wjoin:
            # free up memory usage by clearing out the content each time we switch databases
            document = dict()
            document[db] = dict()

            # based on the database name, call the implemented function to retrieve data and join tables
            match db:
                case 'mouleconnected':
                    for table in self.dict_structure[db].keys():
                        document = self._mouleconnected_to_dict(document=document, database=db, table=table)
                    dataframe = self._join_mouleconnected(document)
                    dataframe = dataframe.groupby('ConvTimeStamp')
                    tms_col_name = 'ConvTimeStamp'
                    del document # free up memory usage by deleting the dict used 
                case 'another_database_of_your_choice':
                    for table in self.dict_structure[db].keys():
                        #! TODO
                        # document = self._otherDB_to_dict(document=document, database=db, table=table)
                    # dataframe = self._join_otherDB(document)
                    # del document
                        pass
                case _:
                    raise "no method implemented yet for this database"
            idx = 0
            for name, group in tqdm(dataframe, total=len(dataframe), desc=f'Processing {db}'):
                group.drop(tms_col_name, axis=1, inplace=True)
                result_dict = {}
                item_key = f'item_{idx}'
                result_dict[item_key] = {}
                result_dict[item_key]['timestamp'] = str(name)
                record_nb = 0
                for index, row in group.iterrows():
                    record_key = f'record_{record_nb}'
                    result_dict[item_key][record_key] = {}
                    for col, value in row.items():
                        result_dict[item_key][record_key][col]= str(value)
                    record_nb +=1
                self._save_dict_to_ndjson(result_dict, db)
                idx += 1

    def _save_dict_to_ndjson(self, data_dict, db_name):
        """
            data_dict is a dictionnary with the following structure
            ```python
                {
                    item_1 : {
                        timestamp : value,
                        records : {
                            record_1 : {
                                field_1 : value,
                                fiel_2 : value,
                                ...
                            }
                        }   
                    },
                    item_2 : {
                        ...
                    }
                }
            ```
        """
        with open(f'test_{db_name}.ndjson', 'a') as f:
            for item_key, item_value in data_dict.items():
                json.dump({item_key: item_value}, f)
                f.write('\n')

    def _mouleconnected_to_dict(self, document, database, table):
        """
            Insert the content of a database table as a dataframe in a dict
            ```python
                {
                    database_name: {
                        table_1: pd.Dataframe,
                        table_2: pd.Dataframe,
                        ...
                    }
                }
            ```
        """
        if table in ['data', 'data2', 'capteur', 'bac']: # keep only useful table
            query = f"SELECT * FROM {database}.{table}" # retrieve content in the table
            result = self.query_mariadb(query=query)
            columns = self.dict_structure[database][table] # retrieve list of columns
            if (table == 'data' or table =='data2'):
                df = pd.DataFrame(result, columns=columns, dtype=pd.Int64Dtype())
            elif table == "capteur":
                df = pd.DataFrame(result, columns=columns).astype({'UniteMesure':str, 'IdMoule': pd.Int64Dtype(), 'IdTemperature': pd.Int64Dtype(), 'IdLumiere': pd.Int64Dtype(), 'IdCapteur':pd.Int64Dtype(), 'IdBac': pd.Int64Dtype()})
            else:
                df = pd.DataFrame(result, columns=columns).astype({'IdBac': int, 'Description': str}) 
            document[database][table] = df
        return document

    def _join_mouleconnected(self,document):
        """
            document is return by _mouleconnected_to_dict()
            document is a dictionnary that contain one dataframe per table in the database

            Return a dataframe with ConvTimeStamp column
        """
        # drop unused column from tables
        if 'IdData' in document['mouleconnected']['data']:
            data = document['mouleconnected']['data'].drop(['IdData', 'DateMesure'], axis=1)
            data2 = document['mouleconnected']['data2'].drop(['IdData', 'DateMesure'], axis=1)
        else :
            data = document['mouleconnected']['data']
            data2 = document['mouleconnected']['data2']

        capteur = document['mouleconnected']['capteur']
        bac = document['mouleconnected']['bac']
        # concatenate the content of data and data2
        data_df = pd.concat([data, data2], ignore_index=True)

        # join data_df with capteur
        data_df = pd.merge(data_df, capteur, on='IdCapteur', how='right')
        # join data_df with bac
        data_df = pd.merge(data_df, bac, on='IdBac', how='right')
        data_df = data_df.sort_values(by='ConvTimeStamp', ascending=False, inplace=False)
        return data_df


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument("database_Wjoin", help="list of databases' names as string that require join operation")
    parser.add_argument("database_Njoin", help="list of databases' names as string that do not require join operation")
    
    args = parser.parse_args()
    database_Wjoin_list = args.database_Wjoin.split(',')
    database_Njoin_list = args.database_Njoin.split(',')

    extract = ExtractData(database_Wjoin=database_Wjoin_list, database_Njoin=database_Njoin_list)
    extract.save_as_ndjson()