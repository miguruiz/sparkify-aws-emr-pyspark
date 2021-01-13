import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, dayofweek, weekofyear, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Dont forget to execute insude EMR cluster.

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data_path = input_data + "/song_data/*/*/*/*.json"

    # read song data file
    df =  spark.read.json(song_data_path)

    # extract columns to create songs table and drop na in primary key column
    songs_table = df.select("song_id","title", "artist_id", "year","duration").dropna(subset = 'song_id')

    # Ensure no duplicates in song_id 
    windowSpec = \
      Window \
    .partitionBy("song_id") \
    .orderBy(col("year").desc(),"duration","title","duration")

    songs_table_uniques = songs_table.withColumn("row_number",row_number().over(windowSpec)).where("row_number = 1")


    # write songs table to parquet files partitioned by year and artist
    songs_table_uniques.write \
        .mode("overwrite") \
        .partitionBy("year","artist_id") \
        .save(output_data +"parquet/songs.parquet", format = 'parquet', header = True)


    # extract columns to create artists table
    artists_table = df.select("year","artist_id","artist_name", "artist_location", "artist_latitude","artist_longitude") \
        .dropna(subset = 'artist_id')

    # Ensure no duplicates in artist_id 
    windowSpec = \
      Window \
    .partitionBy("artist_id") \
    .orderBy(col("year").desc(),"artist_location","artist_name","artist_latitude","artist_longitude")

    artists_table_uniques = artists_table.withColumn("row_number",row_number().over(windowSpec)).where("row_number = 1")

    # write artists table to parquet files
    artists_table_uniques \
        .drop("year") \
        .write \
        .mode("overwrite") \
        .save(output_data + "parquet/artists.parquet", header = True)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data_path = input_data + "/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data_path)

    # extract columns for users table    
    users_table = df.select("userid","firstName","lastName","gender","level").dropDuplicates(['userid']).where(col("userid").isNotNull())

    # write users table to parquet files
    users_table.write \
        .mode("overwrite") \
        .save(output_data + "parquet/users.parquet", header = True)
    
    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)

    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    df = df.withColumn("datetime", get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select('ts','datetime','start_time',
                            hour(df.start_time).alias('hour'),
                            dayofmonth(df.start_time).alias('day'),
                            dayofweek(df.start_time).alias('weekday'),
                            weekofyear(df.start_time).alias('week'),
                            month(df.datetime).alias('month'),
                            year(df.datetime).alias('year')
                          ).dropDuplicates()
    # write time table to parquet files partitioned by year and month
    time_table.write \
        .mode("overwrite") \
        .partitionBy("year","month") \
        .save(output_data + "parquet/time.parquet", header = True)
    
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "/song_data/*/*/*/*.json")
    
    
    # extract columns from joined song and log datasets to create songplays table 
    cond = [df.artist == song_df.artist_name , df.song == song_df.title,  df.length == song_df.duration ]
    songplays_table = df.join(song_df,cond) \
      .withColumn("start_time",get_timestamp(df.ts)) \
      .withColumn("year",year("start_time")) \
      .withColumn("month",month("start_time")) \
      .select("start_time","userid","level","song_id","artist_id","sessionid", "location", "userAgent", "year","month")
      

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
            .mode("overwrite") \
            .partitionBy("year","month") \
            .save(output_data +"parquet/songplays.parquet", format = 'parquet', header = True)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    try:
        print("------- Received: ",sys.argv[1])
        output_data = sys.argv[1]
    except IndexError:
        print "Please provide the direction of the S3 output bucket e.g.: S3://myoutputbucket/'"
        sys.exit(1)
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
