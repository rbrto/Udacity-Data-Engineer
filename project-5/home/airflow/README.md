# Project 5: Data Pipelines with Airflow

### Introduction
This project builds a data pipeline for Sparkify using Apache Airflow that automates and monitors the running of an ETL pipeline.

The ETL loads song and log data in JSON format from S3 and processes the data into analytics tables in a star schema on Reshift. A star schema has been used to allow the Sparkify team to readily run queries to analyze user activity on their app. Airflow regularly schedules this ETL and monitors its success by running a data quality check.

### ELT Process

The tool used for scheduling and orchestrationg ELT is Apache Airflow.

'Airflow is a platform to programmatically author, schedule and monitor workflows.'

Source: [Apache Foundation](https://airflow.apache.org/)

### Sources

The sources are as follows:

* `Log data: s3://udacity-dend/log_data`
* `Song data: s3://udacity-dend/song_data`

### Destinations

Data is inserted into Amazon Redshift Cluster. The goal is populate an star schema:

* Fact Table:

    * `songplays` 

* Dimension Tables

    * `users - users in the app`
    * `songs - songs in music database`
    * `artists - artists in music database`
    * `time - timestamps of records in songplays broken down into specific units`

By the way we need two staging tables:

* `Stage_events`
* `Stage_songs`

### Project Structure

* /
    * `create_tables.sql` - Contains the DDL for all tables used in this projecs
* dags
    * `udac_example_dag.py` - The DAG configuration file to run in Airflow
* plugins
    * operators
        * `stage_redshift.py` - Operator to read files from S3 and load into Redshift staging tables
        * `load_fact.py` - Operator to load the fact table in Redshift
        * `load_dimension.py` - Operator to read from staging tables and load the dimension tables in Redshift
        * `data_quality.py` - Operator for data quality checking
    * helpers
        * `sql_queries` - Redshift statements used in the DAG

### Data Quality Checks

In order to ensure the tables were loaded, 
a data quality checking is performed to count the total records each table has. 
If a table has no rows then the workflow will fail and throw an error message.