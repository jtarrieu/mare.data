# MARE.DATA
## batch_extraction

In this branch handdle the extraction of data from MariaDb and save it into ndjson files

Tasks to achieve :
- [x] Study solution both with Kafka connect and using extract script
- [x] Insert data into mariadb
- [x] Connect to mariadb
- [x] Extract database name
- [x] Extract database structure (table and columns in table)
- [x] Extract data from Mariadb
- [x] Save data in ndjson file


This json-like data will be used in another script to be inserted into ElasticSearch.
As a streaming pipeline will be implemented in parallel to fill ElasticSearch in 'real-time' we need to extract only the data that has been added to MariaDB before the implementation of the streaming pipeline.
