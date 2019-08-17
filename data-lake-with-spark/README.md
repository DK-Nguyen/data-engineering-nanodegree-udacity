# Project: Data Lake with AWS S3 and Spark
## Summary
* [ETL Process with Spark](#ETL-Process-with-Spark)
* [Project structure](#Project-structure) 
* [How to run](#How-to-run)

In this project, we build an ETL pipeline for a user and song database using S3 (data storage) and Spark. We need to load the data from S3, process the data into analytics tables using Spark, and load them back into S3. The Spark process is deployed on a cluster using AWS.

--------------------------------------------
## ETL Process with Spark
The data is stored in a public [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/) with 2 datasets: `song_data` and `log_data`.  
We read the data in JSON files, then using Spark to transform the song files and log files into tables: `songs_table` and `artists_table` are extracted from `song_data`, then written into parquet files in a private S3 bucket.
The similar process is applied to `log_data` to get the `users_table` and `songplays_table`.  
The column `ts` in the `log_data` is also processed into timestamp format and then stored in the `time_table`.

--------------------------------------------
## Project Structure
* <b> data/ </b> - The subset of the datasets hosted on S3. Used for exploration.
* <b> output/ </b> - The local output folder.
* <b> etl.ipynb </b> - The demonstration of the ETL process using local data.
* <b> etl.py </b> - Reads the data from S3, transforms it to the fact and dimension tables and writes it to partitioned parquet files on S3.
* <b> dl.cfg </b> - Config files that contains AWS Credentials information (to connect to ERM and S3 service).

--------------------------------------------
## How to run
Fill in your AWS Credentials in `dl.cfg`, make sure that you have full S3 access right.  
In `etl.py`, change the `output_data` variable to where you want to save the parquets.  
After that, run `etl.py` to execute the ETL process.