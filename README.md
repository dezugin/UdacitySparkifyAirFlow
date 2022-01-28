# Project: Data Pipelines with Airflow

This is project 5 of the udacity data engineering course.

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring me into the project and expected me to create high grade data pipelines that were dynamic and built from reusable tasks, could be monitored, and allowed easy backfills. They have also noted that the data quality played a big part when analyses were executed on top the data warehouse and wanted to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resided in S3 and needed to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consisted of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview

This project introduced me to the core concepts of Apache Airflow. I needed to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

They provided me with a project template that took care of all the imports and provided four empty operators that needed to be implemented into functional pieces of a data pipeline. The template also contained a set of tasks that needed to be linked to achieve a coherent and sensible data flow within the pipeline.

I was  provided with a helpers class that contained SQL transformations. I did not use it.

Example DAGs:

![Example Dag](example-dag.png?raw=true "Example dag")

I connected airflow to AWS, downloaded data from s3 and loaded into redshift.

## Datasets

I worked with two datasets. Here are the s3 links for each:

    Log data: s3://udacity-dend/log_data
    Song data: s3://udacity-dend/song_data

I organized the dataset in  a star schema through a few Airflow operators in a direct acyclic graph:

- Stage Operator: Loaded  JSON formatted files from S3 to Amazon Redshift through a copy query into staging tables.
- Fact and Dimension Operators: loaded files from staging tables to fact and dimension tables
- Data quality operator: tested data quality
- Create tables operator: created tables if they did not exist

Star schema:
![Star Schema](Song_ERD.png?raw=true "Star Schema")
