import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
from prettytable import PrettyTable

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CRED']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CRED']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark



"""
    This procedure extracts metadata of songs stored in S3 in JSON format. 
    It prepares data frames for artists and songs table from this dataset.
    It saves these tables back to S3 in Parquet file format.
    Songs table is partitioned by year of song and artist ID while storing as Parquet file.
    Inputs:
    * spark instance of Spark context
    * input_data file path of directory where datasets for songs and user activity log data are stored
    * output_data file path of directory where the dimensions tables are to be stored as Parquet format
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet", partitionBy=["year", "artist_id"], mode="overwrite")
    
    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as lattitude", "artist_longitude as longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")

    
    
    
"""
    This procedure extracts data about user activity logs on music app stored in S3 in JSON format. 
    It prepares data frames for users, songplays and corresponding timestamps table from this dataset.
    It saves these tables back to S3 in Parquet file format.
    Songplays and timestamp tables are partitioned by year and month while storing as Parquet files.
    
    Inputs:
    * spark instance of Spark context
    * input_data file path of directory where datasets for songs and user activity log data are stored
    * output_data file path of directory where the dimensions tables are to be stored in Parquet format
"""
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("datetime", get_datetime(df.ts))    
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time", "hour(timestamp) as hour", "day(timestamp) as day", "weekofyear(timestamp) as week", "month(timestamp) as month", "year(timestamp) as year", "dayofweek(timestamp) as weekday").dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time.parquet", partitionBy=["year", "month"], mode="overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT  l.timestamp as start_time, 
                l.userId as user_id,
                l.level as level,
                s.song_id as song_id,
                s.artist_id as artist_id,
                l.sessionId as session_id,
                l.location as location,
                l.userAgent as user_agent,
                year(l.timestamp) as year,
                month(l.timestamp) as month
        FROM logs l
         JOIN songs s ON (l.song=s.title AND l.length=s.duration AND l.artist=s.artist_name)
    """).withColumn("songplay_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet", partitionBy=["year", "month"], mode="overwrite")
    
 

"""
    This procedure runs sample query against users table
    It also prints count for each of the dimension tables stored as Parquet files
    Inputs:
    * spark instance of Spark context
    * output_data file path to directory where output database tables are stored
"""
def run_sample_query(spark, output_data):      
    songs = spark.read.parquet(output_data + 'songs.parquet')
    print ("Number of records in songs table: ", songs.count())
    
    artists = spark.read.parquet(output_data + 'artists.parquet')
    print ("Number of records in artists table: ", artists.count())    
    
    time_df = spark.read.parquet(output_data + 'time.parquet')
    print ("Number of records in time table: ", time_df.count())
    
    songplays = spark.read.parquet(output_data + 'songplays.parquet')
    print ("Number of records in songplays table: ", songplays.count())
    
    users = spark.read.parquet(output_data + 'users.parquet')
    print ("Number of records in users table: ", users.count())
    
    users.registerTempTable("user_data")

    gender_paid_counts = spark.sql(
    """
    SELECT 
        COUNT(*) GENDER_COUNT,
        gender, 
        ROUND(AVG(CASE WHEN level = 'paid' THEN 1 ELSE 0 END), 2) PAID_FRACTION
    FROM user_data
    GROUP BY 2
    """).collect()
    table = PrettyTable(['Gender', 'Count', 'Paid Fraction'])
    for row in gender_paid_counts:
        table.add_row([row.gender, row.GENDER_COUNT, row.PAID_FRACTION])
    print(table)

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://atharva-dend/data-lake/"
#     input_data = "s3a://atharva-data-eng/data-lake/small/"
#     output_data = "s3a://atharva-data-eng/data-lake/small/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    run_sample_query(spark, output_data)
if __name__ == "__main__":
    main()
