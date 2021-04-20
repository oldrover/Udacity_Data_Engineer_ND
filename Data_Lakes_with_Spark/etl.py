import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
SONG_DATA_SET = config['DATA']['SONG_DATA_SET']
LOG_DATA_SET = config['DATA']['LOG_DATA_SET']
OUTPUT_DATA = config['DATA']['OUTPUT_DATA']

def create_spark_session():
    """ Creates the Spark session """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads the Songs files, extracts the data for songs table and artists table andsaves it to parquet files
    
    Parameters:
    
    spark: spark session
    input_data: input files path
    output_data: output files path
    """
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("df_songs_table")

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Loads the Log files, extracts the data for users table, time table and songplays table then saves it to parquet files
    
    Parameters:
    
    spark: spark session
    input_data: input files path
    output_data: output files path
    """
    
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : datetime.utcfromtimestamp(int(ts)/1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: to_date(ts), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table   
    df = df.withColumn("hour",hour("start_time"))\
        .withColumn("day",dayofmonth("start_time"))\
        .withColumn("week",weekofyear("start_time"))\
        .withColumn("month",month("start_time"))\
        .withColumn("year",year("start_time"))\
        .withColumn("weekday",dayofweek("start_time"))
    
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday").distinct()              
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table/", mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM df_songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
        .distinct() \
        .select("start_time", "userId", "level", "sessionId", "location", "userAgent","song_id","artist_id", "month", "year") \
        .withColumn("songplay_id", monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(OUTPUT_DATA + "songplays_table/", mode="overwrite", partitionBy=["year", "month"])

def main():
    spark = create_spark_session()  
    output_data = OUTPUT_DATA
    
    process_song_data(spark, SONG_DATA_SET, output_data)    
    process_log_data(spark, LOG_DATA_SET, output_data)


if __name__ == "__main__":
    main()
