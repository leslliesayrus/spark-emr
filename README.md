# Spark in the EMR
ETL example in the world population data using the Spark in the AWS cloud.

![alt text](https://github.com/leslliesayrus/spark-emr/blob/main/aws_archichture.png)

This architecture is a data pipeline to processing in batch, the S3 Ingestion Layer reiceive the data from Data Source. 

The Spark job in the EMR opens two dataframes from S3 Ingestion Layer, the first DataFrame has information about the countries and another DataFrame has information about the number world population, both tables are connected with the primary-foreign key (cod_country).

So the Spark job opens these tables, makes a join and changes some columns, and then saves the processed data in another bucket S3.

After this process, the data is ready to be cataloged by Glue Crawler and analyzed by AWS Athena.
