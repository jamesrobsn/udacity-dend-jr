## Overview

The purpose of this project is to create a data model from various publicly available input sources that can be modeled to build Power BI dashboards. Spark and Airflow would be good to incorporate down the line if the data becomes more unmanageable. For now, python and pandas is able to complete the job in a reasonable amount of time.

## Data Sources

World Temperature Data: https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data
U.S. City Demographic Data: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
EIA Data: https://www.eia.gov/electricity/data/eia923/

## How to Run

Prerequisite:

In order to run this etl process, you must create an AWS Redshift cluster and an AWS IAM role that can access an S3 bucket. Connection details must be entered into the dwh.cfg file for the python etl to run properly.

To run the Python scripts: 

1. Open the Commands section of the Jupyter Notebook and open a New Console
2. Run command "%run create_tables.py"
3. Run command "%run etl.py"

## Files

The raw data files are stored in this repo, converted to csvs, loaded to an AWS S3 bucket specified in the dwh.cfg file then finally copied to a redshift cluster database. It is split between song data and log data. Song data contains json files for each individual song and log data contains log data broken down by day.

The rest of the files in this repo are all used to create the etl process and database schema.

## ETL and Schema Design

Redshift was selected to store and serve the data because it is relatively inexpensive and scales well.

I chose to build a star schema model with fact and dimension tables because this sort of model is perfectly suited for analytics in Power BI. 

The etl process first converts all files to csvs locally. The reason for this is I was having trouble loading directly to the redshift table or s3 directly and this intermediary step helped resolve that. Next, the csvs are loaded to an S3 bucket and then copied from there to staging tables in redshift. Finally, all of the modeled tables are created by querying the staging tables.

This pipeline would need to be run, at most, once per month as the new EIA data is released

## Data Model

![alt text](https://github.com/jamesrobsn/udacity-dend-jr/blob/master/model.png?raw=true)

## Other Scenarios

* The data was increased by 100x
    * I would increase redshift resources and make sure all tables are adequately partitions.
* The pipelines would be run on a daily basis by 7 am every day.
    * I would rebuild the etl process in airflow and make sure that only new data was loaded and/or only updates were made.
* The database needed to be accessed by 100+ people.
    * I would simply scale up my redshift cluster by increaseing cores and nodes available to it.