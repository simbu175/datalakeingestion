# Development notes
This repository serves the purpose of Ingesting MySQL tables to Amazon S3 as data lake.
This code fetches incremental load from MySQL hosted in an EC2 instance

1. Reads an Excel based config from S3 (can be configurable)
2. Loads this excel data to a pandas dataframe based on a condition
3. The previous run logs are stored also in S3 and are updated for each runs
4. These logs are classified at /database-name/table-name prefix layer
5. The current run of ingestion extracts all the live data from this condition
6. This incremental data is stored as a pandas dataframe and loaded into S3 
7. The bucket and prefix information are stored in the Excel config 
8. This package uses `awswrangler`, `AWS SDK - boto3` modules to load and extract data from within AWS
9. It handles parallel loading of tables for efficient ingestion mechanism
10. And this factor can be dependent on the load of the source database as well as the server from which we run these codes
11. Once all the required data is loaded, it automatically calls a Glue Crawler
12. Note that, this crawler must be created beforehand for every new database to be added to data lake 
13. The code waits till all the necessary crawlers are SUCCEEDED
14. After this, a post validation check is ensured for data integrity
15. This check, basically is a counter based which compares the data in the dataframe against the recently loaded data to S3 through Athena's Query API via aws wrangler
16. There is an option to disable this validation at granular level as well
17. For certain tables, to query the latest data, we have an option of creating Athena views where we will mark the latest row per id through row_number() SQL function as per config requirements
18. However, this is optional and unnecessary for smaller tables (or less frequently updated tables)
19. The ingestion logs are updated as SUCCESSFUL for the current iteration
20. This orchestration is currently handled via an Airflow DAG which is hosted within an Airflow server
21. The codes are containerized through docker and can be pushed to a docker instance as well
22. The IAM permissions, pertaining to the below are assumed to be setup as applicable,
    1. Reading data from MySQL
    2. Reading/Writing data to S3 (datalake, config & log updates)
    3. EC2 access for running these codes
    4. Glue crawler API access
    5. Athena Query API access
23. Appropriate error handling mechanisms for updating logs
24. Existing problems and solutions
    1. For truly querying historical and latest data for larger tables, Amazon Athena sometimes errors with timeout
       1. Solution : Need to break down the view into multiple quarter level for query to not return with a memory error
25. Other exploration opportunities
    1. Using LakeFormation's Governed Tables for ACID compliant data lake
    2. Trying with Redshift Spectrum for querying S3 data lake for bigger tables