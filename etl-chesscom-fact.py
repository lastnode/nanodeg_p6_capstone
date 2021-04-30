from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
import argparse
import yaml
from pgn_parser import parser, pgn

with open(r'dl-chesscom.yaml') as file:
    config = yaml.load(file)


def get_eco_from_pgn(pgn_text):

    try:
        game = parser.parse(pgn_text, actions=pgn.Actions())
    
        eco_url = game.tag_pairs["ECOUrl"]

        eco_url_split = eco_url.split("/")

        eco_url_replaced = eco_url_split[-1].replace("-"," ") # Return the last element of the list (the part after the last / character)

        return eco_url_replaced 

    except:
            pass


def get_termination_from_pgn(pgn_text):

    try:
        game = parser.parse(pgn_text, actions=pgn.Actions())
    
        return game.tag_pairs["Termination"]

    except:
            pass


def get_moves_from_pgn(pgn_text):

    try: 
        game = parser.parse(pgn_text, actions=pgn.Actions())
    
        return str(game.movetext)

    except:
            pass


spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()

spark.udf.register("get_eco_from_pgn", get_eco_from_pgn)
spark.udf.register("get_termination_from_pgn", get_termination_from_pgn)
spark.udf.register("get_moves_from_pgn", get_moves_from_pgn)

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

df = spark.read.parquet(output_data + "staging/chessdotcom_local_v3/1*")

df.createOrReplaceTempView("chessdotcom_staging")

moves_table = spark.sql("""

                        select
                            from_unixtime(end_time, 'yyyy-MM-dd hh:mm:ss') as game_end_time,
                            from_unixtime(end_time, 'yyyy-MM-dd') as game_end_date,
                            time_class,
                            rated,
                            white_username,
                            white_rating,
                            black_username,
                            black_rating,
                            case
                                when white_result = "win" then "white"
                                when black_result = "win" then "black"
                                when (white_result = "agree" or black_result = "agree") then "draw"
                                when (white_result = "stalemate" or black_result = "stalemate") then "stalemate"
                            end as winner,
                            get_termination_from_pgn(pgn) as termination,
                            get_eco_from_pgn(pgn) as opening,
                            get_moves_from_pgn(pgn) as moves,
                            'chessdotcom' as platform
                        from chessdotcom_staging

                        order by game_end_time

            """)

#moves_table_cleaned = moves_table.dropDuplicates(['id'])

#moves_table_cleaned.show()

moves_table.write.mode('append').parquet(output_data +"fact/chessdotcom/" + "moves6/")

# 
# openings_table = spark.sql("""
# 
# select
# get_eco_from_pgn(pgn) as eco,
# count(*) as game_count
# from chessdotcom_staging
# 
# group by eco
# order by game_count
# 
# """)
# 
# openings_table.write.mode('append').parquet(output_data +"fact/" + "openings5/")

