from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
import argparse
import yaml
import os
from pgn_parser import parser, pgn

def main():

    with open(r'dl-chesscom.yaml') as file:
        config = yaml.load(file,Loader=yaml.SafeLoader)

    parser = argparse.ArgumentParser(
        prog='etl-staging.py',
        description="""ETL Script that extracts data from
            Lichess API and loads them into a staging table in
            parquet files.""")

    parser.add_argument(
        '-l', '--local',
        action='store_true',
        help="""Save data locally instead of outputting to s3.""")

    args, _ = parser.parse_known_args()

    if args.local:
        output_data = config['output_data_path_local']
    else:
        output_data = config['output_data_path_s3']

    os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']

    spark = SparkSession \
        .builder \
        .appName("Cleaning Lichess data via Spark") \
        .getOrCreate()

    # Setting the MapReduce algorithm to v2, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",config['aws_access_key_id'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",config['aws_secret_key_id'])

    try:
        df = spark.read.parquet(output_data + "staging/lichess/*.parquet")

        df.createOrReplaceTempView("lichess_staging")

        games_table = spark.sql("""

                                select
                                    from_unixtime(lastMoveAt/1000, 'yyyy-MM-dd hh:mm:ss') as game_end_time,
                                    from_unixtime(lastMoveAt/1000, 'yyyy-MM-dd') as game_end_date,
                                    speed as time_class,
                                    players_white_user_name as white_username,
                                    players_white_rating as white_rating,
                                    players_black_user_name as black_username,
                                    players_black_rating as black_rating,
                                    case
                                        when winner = "black" then "black"
                                        when winner = "white" then "white"
                                        when winner = "" then status
                                    end as winner,
                                    status as termination,
                                    opening_name as opening,
                                    moves,
                                    'lichess' as platform
                                from lichess_staging

                                order by game_end_time
                    """)

    except:
        pass


    try: 
        games_table.write.mode('append').parquet(output_data +"staging/lichess/" + "games/")

    except: 
        pass


if __name__ == "__main__":
    main()
