import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
import time

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# Process_song_data with spark
def process_song_data(spark, input_data, output_data):
    """
    read data in the song_data folder, extract and load information into songs_table and artists_table
    write the tables into parquet files
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    song_data1 = input_data + "song_data/*/*/*/TRABCEI128F424C983.json"  # to experiment on 1 file
    
    songdata_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True),        
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)
    df = df.dropDuplicates()
    df.printSchema()
    print('song_data')
    print(f'# rows in song_data: {df.count()}')
    df.show(5)
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    print('songs_table')
    print(f'# rows in songs_table: {songs_table.count()}')
    songs_table.show(5)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path = os.path.join(output_data, "songs_table.parquet")
    if not os.path.exists(songs_table_path):
        songs_table.write.partitionBy("year", "artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", 
                              "artist_latitude", "artist_longitude")
    print('artists_table')
    print(f'# rows in artists_table: {artists_table.count()}')
    artists_table.show(5)
    
    # write artists table to parquet files
    artists_table_path = os.path.join(output_data, "artists_table.parquet")
    if not os.path.exists(artists_table_path):
        artists_table.write.parquet(artists_table_path)


# Process_log_data with spark
def process_log_data(spark, input_data, output_data):
    """
    read data in the log_data folder, extract and load information into users_table and songsplay_table
    extract time stamps in the 'ts' column into the time_table
    write the tables into parquet files
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    log_data1 = input_data + "log_data/*/*/2018-11-12-events.json"

    # define schema
    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", IntegerType(), True),
    ])
    
    # read log data file
    df = spark.read.json(log_data, schema=logdata_schema)
    df = df.dropDuplicates()
    print('log_data')
    print(f'# rows in log_data: {df.count()}')
    df.show(10)
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")
    print('log_data with column page == "NextSong"')
    print(f'# rows in log_data now: {df.count()}')
    df.show(5)
    
    # extract columns for users table    
    users_table = df.select([col("userId").alias("user_id"),
                             col("firstName").alias("first_name"),
                             col("lastName").alias("last_name"),
                             col("gender"),
                             col("level")])
    print('users_table')
    print(f'# rows in users_table: {users_table.count()}')
    users_table.show(5)
    
    # write users table to parquet files
    users_table_path = os.path.join(output_data, "users_table.parquet")
    if not os.path.exists(users_table_path):
        users_table.write.parquet(users_table_path)
    
    tsFormat = "yyyy-MM-dd HH:MM:ss z"
    # Converting ts to a timestamp format
    time_table = df.withColumn('ts',
                               to_timestamp(
                                   date_format(
                                       (df.ts /1000).cast(dataType=TimestampType()), 
                                       tsFormat), 
                                   tsFormat)
                              )
    
    time_table = time_table.select(col("ts").alias("start_time"),
                                   hour(col("ts")).alias("hour"),
                                   dayofmonth(col("ts")).alias("day"), 
                                   weekofyear(col("ts")).alias("week"), 
                                   month(col("ts")).alias("month"),
                                   year(col("ts")).alias("year"))
    print('time_table')
    print(f'# rows in time_table: {time_table.count()}')
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    if not os.path.exists(os.path.join(output_data, 'time')):
        time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'time'))
    
    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = song_df.join(df, song_df.artist_name == df.artist) \
                          .withColumn("songplay_id", monotonically_increasing_id()) \
                          .withColumn('start_time', to_timestamp(date_format(
                                          (col("ts") /1000)
                                           .cast(dataType=TimestampType()), tsFormat),tsFormat))
    
    songplays_table = songplays_df.select("songplay_id",
                                          "start_time",
                                          col("userId").alias("user_id"),
                                          "level",
                                          "song_id",
                                          "artist_id",
                                          col("sessionId").alias("session_id"),
                                          col("artist_location").alias("location"),
                                          "userAgent",
                                          month(col("start_time")).alias("month"),
                                          year(col("start_time")).alias("year"))
    print('songplays_table')
    print(f'# rows in songplays_table: {songplays_table.count()}')
    songplays_table.show(5)
           
    # write songplays table to parquet files partitioned by year and month
    if not os.path.exists(os.path.join(output_data, 'songplays_table')):
        time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'songplays_table'))


def main():
    print('--- Starting Project ---')
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dk-dend-bucket"
    
    process_song_data_start = time.time()
    process_song_data(spark, input_data, output_data)
    process_song_data_end = time.time()
    print(f'time to process song_data: {process_song_data_end - process_song_data_start :.2f}s')
    
    process_log_data_time = time.time()
    process_log_data(spark, input_data, output_data)
    process_log_data_end = time.time()
    print(f'time to process song_data: {process_log_data_end - process_log_data_time :.2f}s')
    
    print('--- Done ---')
    
    
if __name__ == "__main__":
    main()
