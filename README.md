# AdvertiseX Data Engineering Solution

# Overview

This project provides a data engineering solution for AdvertiseX, a digital advertising technology company specializing in programmatic advertising. The solution is designed to handle data ingestion, processing, storage, and monitoring for ad impressions, clicks, conversions, and bid requests.

## Architecture
The solution utilizes AWS services and SnowPipe to create a scalable and efficient data pipeline:

    File Storage: Amazon S3
    Data Ingestion: Snowpipe
    Data Processing: Snowflake
    Data Storage: SnowFlake
    Monitoring: SnowFlake Monitoring


## Data Ingestion

Data is ingested from Amazon S3 buckets to Snowflake in real time using Snowpipe.

Separate buckets and prefixes are used for each data source ( i.e. ad impressions, clicks conversions, and bid requests).

## Data Processing

Snowflake file format transforms and standardizes data, converting JSON, CSV, and Avro data to Table format for efficient querying. 

Snowflake Merge and Snowflake Tasks handle the data transformation for tasks such as data validation, deduplication, and upserting the data to target tables in Snowflake. 

Snowflake query is used for ad-hoc queries to correlate ad impressions with clicks and conversions and create a table.

## Data Storage and Query Performance

Snowflake is used as the data warehouse to store processed data. The table design is optimized for query performance. 
The snowflake stage is used to query files that are present in S3.

## Error Handling and Monitoring

Snowflake Monitoring for monitoring the data pipelines for errors or delays in data ingestion and transformation. 

Snowflake task with auto retry and suspend after no.of failures automatically handles common errors and ingests the data hourly to the table.

Snowflake Monitoring is used for monitoring data pipelines and requires manual intervention if there are any failures or errors.

## Conclusion

This solution provides a robust and scalable data engineering platform for AdvertiseX, enabling the organization to analyze and optimize its 
advertising campaigns effectively.
