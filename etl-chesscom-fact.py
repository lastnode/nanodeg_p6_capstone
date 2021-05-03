from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SQLContext
import argparse
import yaml
import os
from pgn_parser import parser, pgn

with open(r'dl-chesscom.yaml') as file:
    config = yaml.load(file,Loader=yaml.SafeLoader)

os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']

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


def main():

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

    spark = SparkSession \
        .builder \
        .appName("Ingesting Chess.com API via Spark") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')\
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",config['aws_access_key_id'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",config['aws_secret_key_id'])


    spark.udf.register("get_eco_from_pgn", get_eco_from_pgn)
    spark.udf.register("get_termination_from_pgn", get_termination_from_pgn)
    spark.udf.register("get_moves_from_pgn", get_moves_from_pgn)

    df = spark.read.parquet(output_data + "raw/chessdotcom/*")

    df.createOrReplaceTempView("chessdotcom_staging")

    games_table = spark.sql("""

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

    games_table.write.mode('append').parquet(output_data +"staging/chessdotcom/" + "games/")

if __name__ == "__main__":
    main()
