from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
import argparse
import yaml
from pgn_parser import parser, pgn

with open(r'dl-lichess.yaml') as file:
    config = yaml.load(file)


spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()

# Setting the MapReduce algorithm to v2, as suggested by Tran Nguyen here -
# https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 

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

df = spark.read.parquet(output_data + "staging/lichess_local_v1/*")

df.createOrReplaceTempView("lichess_staging")

moves_table = spark.sql("""

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
                            moves
                        from lichess_staging

                        order by game_end_time

            """)

#moves_table_cleaned = moves_table.dropDuplicates(['id'])

#moves_table_cleaned.show()

moves_table.write.mode('append').parquet(output_data +"fact/lichess/" + "moves5/")

openings_table = spark.sql("""

                        select
                            opening_name as opening,
                            from_unixtime(lastMoveAt/1000, 'yyyy') as game_year,
                            count(*) as game_count
                        from lichess_staging

                        group by opening, game_year
                        order by game_count asc

            """)

openings_table.write.mode('append').parquet(output_data +"fact/lichess/" + "openings5/")

