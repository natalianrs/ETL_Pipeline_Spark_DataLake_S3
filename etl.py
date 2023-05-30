import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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


# function process song data to populate tables
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 
                            'title', 
                            'artist_id', 
                            'year', 
                            'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs_table', 
                                mode='overwrite', 
                                partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists_table',
                                mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()    
    
    # write users table to parquet files
    user_table.write.parquet(f'{output_data}/user_table', mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', Function.from_unixtime(Function.col('ts')/1000))
     
    # create datetime column from original timestamp column
    time_table = df.select('ts', 'start_time') \
                   .withColumn('year', Function.year('start_time')) \
                   .withColumn('month', Function.month('start_time')) \
                   .withColumn('week', Function.weekofyear('start_time')) \
                   .withColumn('weekday', Function.dayofweek('start_time')) \
                   .withColumn('day', Function.dayofyear('start_time')) \
                   .withColumn('hour', Function.hour('start_time')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_data}/time_table',
                             mode='overwrite',
                             partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/A/A/A/*.json'
    song_dataset = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # join & extract cols from song and log datasets to create songplays table
    song_dataset.createOrReplaceTempView('song_dataset')
    time_table.createOrReplaceTempView('time_table')
    df.createOrReplaceTempView('log_dataset')
    songplays_table = spark.sql("""SELECT DISTINCT
                                       l.ts as ts,
                                       t.year as year,
                                       t.month as month,
                                       l.userId as user_id,
                                       l.level as level,
                                       s.song_id as song_id,
                                       s.artist_id as artist_id,
                                       l.sessionId as session_id,
                                       s.artist_location as artist_location,
                                       l.userAgent as user_agent
                                   FROM song_dataset s
                                   JOIN log_dataset l
                                       ON s.artist_name = l.artist
                                       AND s.title = l.song
                                       AND s.duration = l.length
                                   JOIN time_table t
                                       ON t.ts = l.ts
                                   """).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table',
                                  mode='overwrite',
                                  partitionBy=['year', 'month'])

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
