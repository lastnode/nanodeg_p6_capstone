from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
import argparse
import yaml

with open(r'dl.yaml') as file:
    config = yaml.load(file)

spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()

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

df = spark.read.parquet(output_data + "staging/*")

df.createOrReplaceTempView("staging")

moves_table = spark.sql("""

                        select
                            from_unixtime(createdAt/1000, 'dd-MM-yyyy hh:mm:ss') as game_start,
                            from_unixtime(lastMoveAt/1000, 'dd-MM-yyyy hh:mm:ss') as game_end,
                            lastMoveAt/1000 - createdAt/1000 as game_length_seconds,
                            id,
                            speed,
                            staging.moves,
                            staging.opening_eco,
                            staging.opening_name,
                            staging.opening_ply,
                            players_black_user_name as black_player_name,
                            players_black_user_title as black_player_title,
                            players_black_rating as black_player_rating,
                            players_white_user_name as white_player_name,
                            players_white_user_title as white_player_title,
                            players_white_rating as white_player_rating,
                            status,
                            winner
                        from staging

            """)

moves_table_cleaned = moves_table.dropDuplicates(['id'])

moves_table_cleaned.show()

moves_table_cleaned.write.mode('append').parquet(output_data +"fact/" + "moves/")

openings_table = spark.sql("""


                    select
                    	opening_name,
                    	count(*) as games
                    from staging

                    group by opening_name
                    order by games
				
				""")

openings_table.write.mode('append').parquet(output_data +"fact/" + "openings/")