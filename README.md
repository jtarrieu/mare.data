<style>
  body {
    font-size: 16px; /* Change the desired font size */
  }
  code {
    font-size: 14px;
  }
</style>

# MARE.DATA
## Extract data from MariaDB| Extraction de la donnée présente sur MariaDB

## [English version](#Englishversion)
## [French version](#Frenchversion)

This branch implement the extraction of data from MariaDb and save it into ndjson files. The ndjson like data will be used in another branch to be inserted into ElasticSearch.
## [English version](#Englishversion)
## CONTENT

- MARE.DATA (root directory)
  - extraction (folder with scripts that implement extraction)
    - extraction.py **(first OOP script for extraction)**
    - opt_extraction.py **(optimized version of extraction.py)**
    - utils.py **(useful functions that should not be implemented in extraction script)**
    - var.py **(variables required to connect to mariadb server)**
    - test_extraction
      - testsExtraction.py **(implementation of unittest for opt_extraction.py)**
  - ressources
    - Définition du processus d'extraction **(how extraction could be implemented)**
    - Procédure de jointure **(how mouleconnected tables has been joined)**
    - Piste_optimisation_extraction **(possible improvement to free up memory in the future)**
    - TESTS unitaires en python **(How to use unittests, others tests realized)**
  - extraction.ipynb **(jupyter notebook ~ extraction.py based on client requirements)**



#### Overall process :
- Insert data into mariadb (POC)
- Connect to mariadb (```Class mariadbConnector```)
- Retrieve databases' structure (databases' names, table and columns inside) (```Class mariadbConnector```)
- Extract data from Mariadb (```Class Extractor```)
- Save data in ndjson file (```Class Extractor```)

### HOW TO ?

### <ins>Insert data into mariadb</ins>

To insert meteo1.sql and mouleconnected.sql files into the instance of Mariadb we created through docker. We realize the following steps :

1. Move the file into mariadb container : 

<code>docker cp your_local_file.sql mariadb_container_name:/path/in/container/your_local_file.sql</code>

2. Access the container console : 

<code>docker exec -it mariadb_container_name /bin/bash</code>

3. From the mariadb container's console run the following code : 

<code>mysql -u your_username -p < /path/in/container/your_local_file.sql</code>

### <ins>Run opt_extraction</ins>

⚠️ You must activate the environment of the project : 
- Windows : ```.\maredataEnv\Scripts\activate```
- Linux : ```source ./maredataEnv/bin/activate```

Navigate to the root folder of the extraction branch.
From the console run :

```python -m extraction.opt_extraction "meteo1" "mouleconnected"```

The first argument corresponds to databases that require join ; the second correspond to databases that do not require join operation. In production they will probably be several databases of each type, to pass several databases, simply use ',' between each databases' name like this :

```python -m extraction.opt_extraction "meteo1,anotherDB" "mouleconnected,anotherDB"```

### <ins>Test</ins>

⚠️ You must activate the environment of the project : 
- Windows : ```.\maredataEnv\Scripts\activate```
- Linux : ```source ./maredataEnv/bin/activate```

To run tests navigate to the root folder of the extraction branch and run the following code : 

```python -m unittest .\extraction\test_extraction\testsExtraction.py```


**⚠️As a streaming pipeline will be implemented in parallel to fill ElasticSearch in 'real-time' we need to extract only the data that has been added to MariaDB before the implementation of the streaming pipeline. Add a WHERE statement in SQL query based on timestamp value.**


## [Version en français](#Frenchversion)

Cette branche implémente l'extraction de la donnée présente sur MariaDB et la sauvegarde dans des fichiers ndjson. L'insertion des fichiers ndjson dans ElasticSearch est prise en charge dans une autre branche du projet.


## CONTENU

- MARE.DATA (racine)
  - extraction (dossier contenant les scripts pour l'extraction)
    - extraction.py **(première version de l'extraction en POO)**
    - opt_extraction.py **(version optimisée de extraction.py)**
    - utils.py **(fonctions nécessaires mais sans lien direct avec l'extraction)**
    - var.py **(variables nécessaire à la connexion au serveur mariaDB)**
    - test_extraction
      - testsExtraction.py **(implementation des tests unitaires pour opt_extraction.py)**
  - ressources
    - Définition du processus d'extraction **(étude des implémentation possible pour l'extraction)**
    - Procédure de jointure **(Procédure de jointure des tables de mouleconnected)**
    - Piste_optimisation_extraction **(pistes d'optimisation pour réduire l'utilisation de la mémoire RAM)**
    - TESTS unitaires en python **(Conseils pour l'implémentation des tests unitaires, présentations des autres tests réalisés)**
  - extraction.ipynb **(jupyter notebook ~ extraction.py conformément aux attendus clients)**



#### Processus général :
- Insérer la donnée dans Mariadb (POC)
- Connexion à mariadb (```Class mariadbConnector```)
- Déterminer la structure des BDD (noms des BDD, tables et colonnes à l'intérieur) (```Class mariadbConnector```)
- Extraire la donnée de Mariadb (```Class Extractor```)
- Sauvergarder la donnée au format ndjson (```Class Extractor```)

### HOW TO ?

### <ins>Insérer la donnée dans Mariadb </ins>

Pour insérer la donnée de meteo1.sql et mouleconnected.sql dans l'instance de Mariadb créée avec Docker. Suivre les étapes ci-après :

1. Placer les fichiers dans le container MariaDB : 

<code>docker cp your_local_file.sql mariadb_container_name:/path/in/container/your_local_file.sql</code>

2. Accèder au terminal du container MariaDB : 

<code>docker exec -it mariadb_container_name /bin/bash</code>

3. Depuis la console de MariaDB executer le code suivant : 

<code>mysql -u your_username -p < /path/in/container/your_local_file.sql</code>

### <ins>Run opt_extraction.py</ins>

⚠️ Il est nécessaire d'acitiver l'envirronment suivant : 
- Windows : ```.\maredataEnv\Scripts\activate```
- Linux : ```source ./maredataEnv/bin/activate```

Se placer à la racine du dossier de la branche d'extraction.
Depuis le terminal exécuter le code suivant :

```python -m extraction.opt_extraction "meteo1" "mouleconnected"```

<p class="text-justify"> Le premier argument correspond aux bases de données qui nécessitent une jointure ; le second correspond aux bases de données qui ne nécessitent pas d’opération de jointure. En production, il y aura probablement plusieurs bases de données de chaque type, pour passer plusieurs bases de données, il suffit d’utiliser « ,» entre chaque nom de base de données comme ceci : </p>

```python -m extraction.opt_extraction "meteo1,anotherDB" "mouleconnected,anotherDB"```

### <ins>Test</ins>

⚠️ Il est nécessaire d'acitiver l'envirronment suivant : 
- Windows : ```.\maredataEnv\Scripts\activate```
- Linux : ```source ./maredataEnv/bin/activate```

Se placer à la racine du dossier de la branche d'extraction. Depuis le terminal exécuter le code suivant :

```python -m unittest .\extraction\test_extraction\testsExtraction.py```


**⚠️Etant donnée qu'une pipeline de streaming (récupération de la donnée en temps réelle) est destinée à être mise en place : il est nécessaire de n'extraire que la donnée précédent la mise en place de cette dernière afin d'éviter des doublons. Pour cela configurer une clause WHERE sur les timestamp dans les requêtes SQL.**