from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pathlib import Path
import requests, json
from datetime import datetime
import os
from flatten_json import flatten
import asyncio
import configparser
import yaml
import argparse
from chessdotcom import get_player_games_by_month, get_player_game_archives
import collections
import pandas as pd
from pyspark.sql.types import StructType,StructField,StringType,BooleanType

with open(r'dl-chesscom.yaml') as file:
    config = yaml.load(file)

os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']


def load_json_files_to_staging(url, local):

    print("Fetching data from: " + url)

    if local == True:
        output_data = config['output_data_path_local']
    else:
        output_data = config['output_data_path_s3']

    spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()

    # Setting the MapReduce algorithm to v2, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 

    response = requests.get(url).text

    games = json.loads(response)

    games = games.pop('games')

    full_flattened_json = []

    pd_df = pd.DataFrame([])

    for game in games:

        flattened_json = pd.json_normalize(game, sep="_")

        full_flattened_json.append(flattened_json)   

        print(type(flattened_json))

        pd_df = pd_df.append(flattened_json)
           
    print(pd_df)

    #url_cleaned = url.replace("//","").replace(".","").replace("https","").replace("/","")

    url_split = url.split("/")

    pd_df.to_parquet(output_data + "staging/chessdotcom_local_v3/" + url_split[5] + "_" + url_split[7] + "_" + url_split[8] + '.parquet')


    #spark_main_df = spark.createDataFrame(pd_df)

    #spark_main_df.write.mode('append').parquet(output_data + "staging/chessdotcom_local_v3/")


def get_chesscom_games(player_list, max_games, local):

    for player in player_list:

        player_archives = get_player_game_archives(player)

        for urls in player_archives.json.values():

            for url in urls:

                load_json_files_to_staging(url, local)


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

    players = config['players']

    print(players)

    max_games = config['max_games_per_player']

    if args.local:
        print("Saving outputd data locally instead of writing to s3 bucket.")

        get_chesscom_games(players, max_games, True)
    else:
        get_chesscom_games(players, max_games, False)

if __name__ == "__main__":
    main()
