# -*- coding: utf-8 -*-
"""azer.ipynb
# Extract data from MariaDB and save it in ndjson files

### Import librairies
"""

import mariadb # librairie pour se connecter à mariadb
import sys
import var # import variable configured into var.py
import pandas as pd
import random
import os
import json
from copy import deepcopy

print(var.user)
print(var.password)
print(var.host)

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user=var.user,
        password=var.password,
        host=var.host,
        port=3306,
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()

cur.execute("SHOW DATABASES;") #* require specific rights on mariadb server

result = cur.fetchall() # retrieve all the results

cur.close()
# Create a list of databases name on the server
dbs_list= []
for row in result:
    dbs_list.append(row[0])

#! TODO : complete unwanted_db with any database that should not be transfered into Elastic
unwanted_db = {'information_schema','mysql','sys','performance_schema'}

#* dbs_list contain all databases that will be taken into account for batch
dbs_list = [db for db in dbs_list if db not in unwanted_db]

print(dbs_list)

# Get tables inside Databases
# We create a dictionnary <code>dict_db</code> with database name as key and value being a list  of tables inside the database


# for each db we want to transfer, we store tables names in the dictionnary
dict_db = {}
for db in dbs_list:
    cur = conn.cursor()
    query = f"SHOW TABLES FROM {db}"
    cur.execute(query)
    result = cur.fetchall()
    result = [value[0] for value in result] # keep only the name of the table
    dict_db[db] = result
    cur.close()

print(dict_db)

# Convert dict of list into dict of dict with list for values extracted

dict_db_tables = {}
for key, values in dict_db.items():
    dict_db_tables[key] = {value: {} for value in values}
print(dict_db_tables)

# Retrieve columns in tables

dict_col_in_table = deepcopy(dict_db_tables)

for db in dict_db.keys():
    for table in dict_db[db]:
        cur = conn.cursor()
        query = f"SHOW COLUMNS FROM {db}.{table}"
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        cols = [value[0] for value in result]
        i=0
        for i in range(len(cols)):
            dict_col_in_table[db][table][cols[i]] = []

dict_col_in_table

dict_db_tables

#? get columns inside each tables and store it inside a dictionnary with arrays as values
# for db in dict_db.keys():
#     for table in dict_db[db]:
#         cur = conn.cursor()
#         query = f"SHOW COLUMNS FROM {db}.{table}"
#         cur.execute(query)
#         result = cur.fetchall()
#         cols = [value[0] for value in result]
#         i=0
#         for i in range(len(cols)):
#             dict_db_tables[db][table][cols[i]] = []

# print((dict_db_tables)


# for db in dict_db.keys(): # iteration selon les bdd
#     for table in dict_db[db]: # pour chaque tables de chaque BDD
#         cur = conn.cursor()
#         query = f"SELECT * FROM {db}.{table}" # extraction de l'intégralité des données
#         cur.execute(query)
#         result = cur.fetchall()
#         rowNb = 0
#         columnNb = 0
#         for col in dict_db_tables[db][table]: # pour chaque colonne dans la table
#             for rowNb in range(len(result)): # pour chaque ligne dnas la table
#                 dict_db_tables[db][table][col].append(result[rowNb][columnNb]) # on ajoute à la liste la donnée de la ligne correspondant à la colonne actuelle
#             columnNb+=1 # on passe à la colonne suivante

list_db_nojoin = ['meteo']
list_db_withjoin = [ x for x in dbs_list if x not in list_db_nojoin] # from the list containing all databases we retrieve only db that are not those that do not require join



#! TODO create two lists of databases, one for db with disjoined tables ; one db with tables to join
for db in list_db_nojoin:
    for table in dict_db[db]: # pour chaque tables de chaque BDD
        cur = conn.cursor()
        query = f"SELECT * FROM {db}.{table} ORDER BY utctimestamp ASC " # extraction de l'intégralité des données
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        rowNb = 0
        for rowNb in range(len(result)): # pour chaque ligne dans la table
            item = f'item_{rowNb}'
            dict_db_tables[db][table][item] = {}
            columnNb = 0
            for col in dict_col_in_table[db][table]: # pour chaque colonne dans la table
                dict_db_tables[db][table][item][col] = result[rowNb][columnNb] # on ajoute à la liste la donnée de la ligne correspondant à la colonne actuelle
                columnNb+=1 # on passe à la colonne suivante

dict_db_tables['meteo']['ane0']['item_14713880']


# select a batch_size
def select_batch_size(n):
    diviseurs = []
    for i in range(20, 500000):
        if n % i == 0:
            diviseurs.append(i)
            print(i)
    return random.choice(diviseurs)

select_batch_size(194998)

# fonction qui convertit les nan en None car les nan ne sont pas supportés en json
def nan_serializer(obj):
        if pd.isna(obj):
                return None
        return obj


for db in list_db_nojoin:
    folder_path = '/data/' + db
    os.makedirs(folder_path, exist_ok=True)
    for table in dict_db_tables[db] :
        file_path = f'{db}/{table}.ndjson'
        lenght = len(dict_db_tables[db][table].items())
        if lenght <= 500000:
            for key, value in dict_db_tables[db][table].items():
                with open(file_path, 'a') as f:
                    f.write(json.dumps({key:value}) + '\n')
        else :
            batch_size = select_batch_size(lenght)
            # on crée une liste contenant chacun des éléments du dictionnaire afin de pouvoir les parcourir
            items = list(dict_db_tables[db][table].items())
            batches = [items[i:i+batch_size] for i in range(0,lenght, batch_size)]
            # on parcourt les lots
            for i, batch in enumerate(batches):
                # on enregistre le contenu du lot dans un fichier
                with open(file_path, 'a') as f:
                    for key, value in batch:
                        entry = {key:value}
                        f.write(json.dumps(entry, default=nan_serializer) + '\n')

df_list = {}
for db in list_db_withjoin:
    df_list[db]={}
    for table in dict_db[db]:
        if db == 'mouleconnected':
            if table in ['data', 'data2', 'capteur', 'bac']:
                cur = conn.cursor()
                query = f"SELECT * FROM {db}.{table}"
                cur.execute(query)
                result = cur.fetchall()
                columns = [col for col in dict_col_in_table[db][table].keys()]
                cur.close()
                if (table == 'data' or table =='data2'):
                    df = pd.DataFrame(result, columns=columns, dtype=pd.Int64Dtype())
                elif table == "capteur":
                    df = pd.DataFrame(result, columns=columns).astype({'UniteMesure':str, 'IdMoule': pd.Int64Dtype(), 'IdTemperature': pd.Int64Dtype(), 'IdLumiere': pd.Int64Dtype(), 'IdCapteur':pd.Int64Dtype(), 'IdBac': pd.Int64Dtype()})
                else:
                    df = pd.DataFrame(result, columns=columns).astype({'IdBac': int, 'Description': str})
                df_list[db][table] = df

df_list['mouleconnected']['data']

def join_mouleconnected():
    # drop unused column from tables
    if 'IdData' in df_list['mouleconnected']['data']:
        data = df_list['mouleconnected']['data'].drop(['IdData', 'DateMesure'], axis=1)
        data2 = df_list['mouleconnected']['data2'].drop(['IdData', 'DateMesure'], axis=1)
    else :
        data = df_list['mouleconnected']['data']
        data2 = df_list['mouleconnected']['data2']

    capteur = df_list['mouleconnected']['capteur']
    bac = df_list['mouleconnected']['bac']
    # concatenate the content of data and data2
    data_df = pd.concat([data, data2], ignore_index=True)

    # join data_df with capteur
    data_df = pd.merge(data_df, capteur, on='IdCapteur', how='right')
    # join data_df with bac
    data_df = pd.merge(data_df, bac, on='IdBac', how='right')
    return data_df

test = join_mouleconnected()

test.sort_values(by='ConvTimeStamp', ascending=False, inplace=True)
print(test.head())
print(test.info())


mouleconnected = {}
i = 0
mouleconnected['state'] = 0
for index, row in test.iterrows():
    if pd.notna(row['ConvTimeStamp']):
        if row['ConvTimeStamp'] != mouleconnected['state']:
            mouleconnected['state'] = row['ConvTimeStamp']
            item = f"item_{i}"
            i+=1
            mouleconnected[item] = {}
            mouleconnected[item]['timestamp'] = row['ConvTimeStamp']
            mouleconnected[item]['records'] = {}
            record_nb = 0
        else:
            record_nb += 1
        record_value = f'record_{record_nb}'
        mouleconnected[item]['records'][record_value]={}
        for col, value in row.items():
            if col != 'ConvTimeStamp':
                mouleconnected[item]['records'][record_value][col] = value

len(mouleconnected)



# on crée une liste contenant chacun des éléments du dictionnaire afin de pouvoir les parcourir
items = list(mouleconnected.items())

# on crée des lots de 74 éléments du dictionnaire
batches = [items[i:i+74] for i in range(1,len(mouleconnected), 74)]

# on parcourt les lots
for i, batch in enumerate(batches):
    # on enregistre le contenu du lot dans un fichier
    with open('mouleconnected.ndjson', 'a') as f:
        for key, value in batch:
            entry = {key:value}
            f.write(json.dumps(entry, default=nan_serializer) + '\n')

# mouleconnectedBatch = {}
# i = 0
# mouleconnected['state'] = 0
# for group_key, group_chunk in test.groupby(by='ConvTimeStamp'):
#     print(group_key, group_chunk)
