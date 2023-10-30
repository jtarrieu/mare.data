# MARE.DATA
## batch_extraction

In this branch we implement :
1. the extraction of data from mariadb database in batches (extract.ipynb)
2. the conversion from csv like into json (extract.ipynb)

This json-like data will be used in another script to be inserted into ElasticSearch.
As a streaming pipeline will be implemented in parallel to fill ElasticSearch in 'real-time' we need to extract only the data that has been added to MariaDB before the implementation of the streaming pipeline.
