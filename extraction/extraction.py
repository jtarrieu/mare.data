import mariadb # librairie pour se connecter à mariadb
import sys
import var # import variable configured into var.py
import pandas as pd
import os
import json
import utils

class MariadbConnector:
    """
        Class that implement a connector for Mariadb
        Query function that open and close cursor
        Retrieve databases, tables and columns on the server
    """

    def __init__(self) -> None:
        self.dict_structure = self._get_structure()

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
                                port=3308,
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
            if database[0] not in unwanted_database:
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
        for database in self._get_databases_list():
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
        Implement the extraction of data from all database on the server 
    """
    def __init__(self) -> None:
        super().__init__()
        self.database_to_join = ['mouleconnected']
        self.database_no_join = ['meteo1']

    def _get_data(self, document, database, table) -> dict:
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
        query = f"SELECT * FROM {database}.{table} ORDER BY utctimestamp ASC LIMIT 50" # extraction de l'intégralité des données
        results = self.query_mariadb(query=query)
        rowNb = 0
        for rowNb in range(len(results)): # pour chaque ligne dans la table
            item = f'item_{rowNb}'
            document[database][table][item] = {}
            columnNb = 0
            for col in self.dict_structure[database][table]: # pour chaque colonne dans la table
                        document[database][table][item][col] = results[rowNb][columnNb] # on ajoute à la liste la donnée de la ligne correspondant à la colonne actuelle
                        columnNb+=1 # on passe à la colonne suivante
        return document

    def _save_as_ndjson(self, batch_size=500000):
        """
            extract data contained in databases that do not require JOIN operation
            data is saved in the dict_table_in_database
        """
        self._save_no_join(batch_size=batch_size)
        self._save_with_join(batch_size=batch_size/2)


    def _save_no_join(self, batch_size):
        for db in self.database_no_join:
            current_dir = os.getcwd()
            folder_path = current_dir + '/' + db
            os.makedirs(folder_path, exist_ok=True)
            document = dict()
            document[db] = dict()
            for table in self.dict_structure[db].keys():
                file_path = f'{db}/{table}.ndjson'
                document[db][table] = dict()
                document = self._get_data(document=document, database=db, table=table)
                lenght = len(document[db][table].items())
                items = list(document[db][table].items())
                batches = [items[i:i+batch_size] for i in range(0,lenght, batch_size)]
                for i, batch in enumerate(batches):
                        # on enregistre le contenu du lot dans un fichier
                        with open(file_path, 'a') as f:
                            for key, value in batch:
                                entry = {key:value}
                                f.write(json.dumps(entry, default=utils.nan_serializer) + '\n')
    
    def _save_with_join(self):
         for db in self.database_to_join:
            document = dict()
            document[db] = dict()
            match db:
                case 'mouleconnected':
                    for table in self.dict_structure[db].keys():
                        document = self._mouleconnected_to_dict(document=document, database=db, table=table)
                    dataframe = self._join_mouleconnected(document)
                    del document
                case 'another_database_of_your_choice':
                    pass
                case _:
                    raise "database not found"
            content = self._df_to_dict(dataframe)
            self._save_dict_to_ndjson(content)

    def _df_to_dict(self, dataframe):
        """
            take a dataframe as entry and return a dict
            state is used only to identify if actual record has the same timestamp as the previous one or not
            item_nb contains all records from a same timestamp
            ```python
            {
                'state' : identifier_value,
                'item_0': {
                    'timestamp' : value,
                    'records' : {
                        'record_0':{
                            dataframe_col1 : value,
                            dataframe_col2 : value,
                            ...
                        },
                        'record_1':{
                            dataframe_col1 : value,
                            dataframe_col2 : value,
                            ...
                        }
                    }
                },
                'item_1' :{
                    ...
                }
            }
            ```
        """
        df_as_dict = dict()
        i = 0
        df_as_dict['state'] = 0
        for index, row in dataframe.iterrows():
            if pd.notna(row['ConvTimeStamp']):
                if row['ConvTimeStamp'] != df_as_dict['state']:
                    df_as_dict['state'] = row['ConvTimeStamp']
                    item = f'item_{i}'
                    i+=1
                    df_as_dict[item] = {}
                    df_as_dict[item]['timestamp'] = row['ConvTimeStamp']
                    df_as_dict[item]['records'] = {}
                    record_nb = 0
                else:
                    record_nb += 1
                record_value = f'record_{record_nb}'
                df_as_dict[item]['records'][record_value]={}
                for col, value in row.items():
                    if col != 'ConvTimeStamp':
                        df_as_dict[item]['records'][record_value][col] = value 
        return df_as_dict

    def _save_dict_to_ndjson(self, data_dict):
        items = list(data_dict.items())
        # on crée des lots de 74 éléments du dictionnaire
        batches = [items[i:i+74] for i in range(1,len(data_dict), 74)]
        # on parcourt les lots
        for i, batch in enumerate(batches):
        # on enregistre le contenu du lot dans un fichier
            with open('test_mouleconnected.ndjson', 'a') as f:
                for key, value in batch:
                    entry = {key:value}
                    f.write(json.dumps(entry, default=utils.nan_serializer) + '\n')

    def _mouleconnected_to_dict(self, document, database, table):
        """
            
        """
        if table in ['data', 'data2', 'capteur', 'bac']:
            query = f"SELECT * FROM {database}.{table} LIMIT 50"
            result = self.query_mariadb(query=query)
            columns = self.dict_structure[database][table]
            if (table == 'data' or table =='data2'):
                df = pd.DataFrame(result, columns=columns, dtype=pd.Int64Dtype())
            elif table == "capteur":
                df = pd.DataFrame(result, columns=columns).astype({'UniteMesure':str, 'IdMoule': pd.Int64Dtype(), 'IdTemperature': pd.Int64Dtype(), 'IdLumiere': pd.Int64Dtype(), 'IdCapteur':pd.Int64Dtype(), 'IdBac': pd.Int64Dtype()})
            else:
                df = pd.DataFrame(result, columns=columns).astype({'IdBac': int, 'Description': str}) 
            document[database][table] = df
        return document

    def _join_mouleconnected(self,document):
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
        return data_df.sort_values(by='ConvTimeStamp', ascending=False, inplace=True)
    
if __name__ == 'main':
