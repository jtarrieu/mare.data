# MARE.DATA
## batch_extraction

Tasks to achieve :
- [x] Study solution both with Kafka connect and using extract script
- [x] Insert data into mariadb
- [ ] Define batches size and which data need to be extracted
- [x] Connect to mariadb
- [x] Extract data from mariadb
- [ ] Unify tables from the same database into one document
- [ ] Convert data into ndjon format


In this branch we implement :
1. the extraction of data from mariadb database in batches (extract.ipynb)
2. the conversion from csv like into json (extract.ipynb)

This json-like data will be used in another script to be inserted into ElasticSearch.
As a streaming pipeline will be implemented in parallel to fill ElasticSearch in 'real-time' we need to extract only the data that has been added to MariaDB before the implementation of the streaming pipeline.
