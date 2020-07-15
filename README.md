# dl_auto_catalog
- repo for relational db scraping related to Manning book **Deep Learning with Structured Data** https://www.manning.com/books/deep-learning-with-structured-data
- the code in this repo extracts table metadata from the catalog of a Postgres database so it can be used to automatically train deep learning models with combinations of data in the database

## Directory structure
- **data** - processed datasets and pickle files for intermediate datasets
- **models** - saved trained models
- **notebooks** - code
- **pipelines** - pickled pipeline files

## To exercise the code

1. Install Postgres https://www.postgresql.org/download/ including pgadmin
2. follow instructions to create sample db: https://www.postgresqltutorial.com/postgresql-sample-database/
3. once you have created the sample db, update the config file notebooks/scrape_db_catalog_config.yml to ensure that the user, host, port and database settings are correct
general:
   user: "postgres" # user ID for the connection
   host: "127.0.0.1" # host for the database - default is localhost
   port: "5432"	# port for the database
   database: "dvdrental" # database name
4. run notebooks/scrape_db_catalog.py. This module will:
- connect to the database using the credentials you specified in the config file
- run a query to get details about the columns of every table in the specified schema
- save the results of the query in a dataframe that gets persisted as a pickle file


## Background

- Postgres catalog: https://pynative.com/python-postgresql-tutorial/
- 