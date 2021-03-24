from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pathlib import Path
import requests, json
from datetime import datetime
import os
from flatten_json import flatten

    
conf = SparkConf()
conf.setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def load_json_files_to_staging(url, nbgames):

    spark = SparkSession \
    .builder \
    .appName("DataCleansing") \
    .getOrCreate()

    params = {'max': nbgames, 'opening': 'true'}

    headers = {'Accept': 'application/x-ndjson'}

    response = requests.get(url, headers=headers, params=params).text

    json_lines = response.splitlines()

    json_data = []

    for line in json_lines:
        json_data.append(json.loads(line))
 
    flattened_json_responses = flatten_json(json_data)

    try:
        df = spark.createDataFrame(flattened_json_responses)

    except ValueError:
        pass
        
    df.show()

    df.createOrReplaceTempView("lichess_raw")

    Path("output_data/lichess_raw/").mkdir(parents=True, exist_ok=True)

    df.write.mode('append').parquet("output_data/" + "lichess_raw/")

    staging_table = spark.sql("""

                    select
                        from_unixtime(createdAt/1000, 'dd-MM-yyyy hh:mm:ss') as game_start,
                        from_unixtime(lastMoveAt/1000, 'dd-MM-yyyy hh:mm:ss') as game_end,
                        lastMoveAt/1000 - createdAt/1000 as game_length_seconds,
                        id,
                        speed,
                        moves,
                        opening_name,
                        opening_ply,
                        players_black_user_name as black_player_name,
                        players_black_user_title as black_player_title,
                        players_black_rating as black_player_rating,
                        players_white_user_name as white_player_name,
                        players_white_user_title as white_player_title,
                        players_white_rating as white_player_rating,
                        status,
                        winner
                    from lichess_raw
        """)

    staging_table.show()

    Path("output_data/staging/").mkdir(parents=True, exist_ok=True)

    staging_table.write.mode('append').parquet("output_data/" + "staging_table/")

 

def flatten_json(json_responses):

    full_flattened_json = []

    for response in json_responses:

        flattened_json = flatten(response)

        print(flattened_json)

        full_flattened_json.append(flattened_json)   

    return full_flattened_json

def get_lichess_games(player_list, nbgames):

    for player in player_list:

        load_json_files_to_staging("https://lichess.org/api/games/user/" + player, nbgames)

def main():

    players = ["alireza2003", "Konevlad", "neslraCsungaM77", "Vladimirovich9000", "sp1cycaterpillar", "Federicov93", "may6enexttime", "Kelevra317", "nihalsarin2004", " Drvitman", "DrNykterstein", "C9C9C9C9C9", "muisback", "Inventing_Invention", "RebeccaHarris", "drop_stone", "Alexander_Zubov", "IWANNABEADOORED", "Kelevra317", "dolar9", "cutemouse83"]

    nbgames = 10

    get_lichess_games(players, nbgames)

if __name__ == "__main__":
    main()
