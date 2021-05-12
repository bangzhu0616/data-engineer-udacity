import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process the song_data and creates songs and artist tables, writing them in parquet files.
    Args:
        - spark: the Spark session
        - input_data: directory containing the data used as input
        - output_data: a directory where the output tables should be stored
    '''

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # test with one data file
    # song_data = 's3a://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json'
    
    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView('song_data_table')

    # extract columns to create songs table
    song_data = spark.sql("""
                          SELECT sd.song_id,
                                 sd.title,
                                 sd.artist_id,
                                 sd.year,
                                 sd.duration
                          FROM song_data_table sd
                          WHERE song_id IS NOT NULL
                          """)
    
    # write songs table to parquet files partitioned by year and artist
    song_data.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT DISTINCT sd.artist_id,
                                              sd.artist_name,
                                              sd.artist_location,
                                              sd.artist_latitude,
                                              sd.artist_longitude
                              FROM song_data_table sd
                              WHERE sd.artist_id IS NOT NULL
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    '''
    Process the song_data and creates songs and artist tables, writing them in parquet files.
    Args:
        - spark: the Spark session
        - input_data: directory containing the data used as input
        - output_data: a directory where the output tables should be stored
    '''

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # test with one data file
    # log_data = input_data + 'log_data/2018/11/2018-11-12-events.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    df.createOrReplaceTempView('log_data_table')

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT ld.userId, 
                                            ld.firstName,
                                            ld.lastName,
                                            ld.gender,
                                            ld.level
                            FROM log_data_table ld
                            WHERE ld.userId IS NOT NULL
                            """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                           SELECT start.start_time as start_time,
                                  hour(start.start_time) as hour,
                                  dayofmonth(start.start_time) as day,
                                  weekofyear(start.start_time) as week,
                                  month(start.start_time) as month,
                                  year(start.start_time) as year,
                                  dayofweek(start.start_time) as weekday
                           FROM
                            (SELECT to_timestamp(ld.ts/1000) as start_time
                             FROM log_data_table ld
                             WHERE ld.ts IS NOT NULL
                            ) start
                           """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                       to_timestamp(ld.ts/1000) as start_time,
                                       month(to_timestamp(ld.ts/1000)) as month,
                                       year(to_timestamp(ld.ts/1000)) as year,
                                       ld.userId as user_id,
                                       ld.level as level,
                                       sd.song_id as song_id,
                                       sd.artist_id as artist_id,
                                       ld.sessionId as session_id,
                                       ld.location as location,
                                       ld.userAgent as user_agent
                                FROM log_data_table ld
                                JOIN song_data_table sd on ld.artist = sd.artist_name and ld.song = sd.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-lake-test/sparkoutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
