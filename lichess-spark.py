from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
import requests, json
from datetime import datetime
import os
from flatten_json import flatten

    
conf = SparkConf()
conf.setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def load_json_files_to_spark(url, nbgames):

    spark = SparkSession \
    .builder \
    .appName("DataCleansing") \
    .getOrCreate()

    params = {'max': nbgames}

    headers = {'Accept': 'application/x-ndjson'}

    response = requests.get(url, headers=headers, params=params).text

    json_lines = response.splitlines()

    json_data = []

    for line in json_lines:
        json_data.append(json.loads(line))
 
    flattened_json_responses = flatten_json(json_data)

    df = spark.createDataFrame(flattened_json_responses)

    df.show()

    df.createOrReplaceTempView("lichess_main")

    moves_table = spark.sql("""

                    select
                        from_unixtime(createdAt/1000, 'dd-MM-yyyy') as timestamp,
                        id,
                        speed,
                        moves,
                        case             
                            when moves like "%d4 Nf6%" then "A45-A46 Queen's pawn game" 
                            when moves like "%d4 d5%" then "D00 Queen's pawn game"                                                                                                              
                            when moves like "%d4 d5%" then "D00 Queen's pawn game"                                                                                                              
                            when moves like "%e4 e5 Nf3 Nc6 Bb5%" then "C60-C99 Ruy Lopez (Spanish opening)"
                            when moves like "%e4 c5%" then "B20-B99 Sicilian defence"
                            when moves like "%e4 c6%" then "B10-B19 Caro-Kann defence"
                            when moves like "%d4 d5 c4 dxc4%" then "D20 Queen's gambit accepted"
                            when moves like "%d4 d5 c4 e6%" then "D30-D42 Queen's gambit declined"                            
                            when moves like "%d4 Nf6 c4 e6 Nf3%" then "E10 Queen's pawn game"                            
                            when moves like "%e4 e5%" then "C20 King's pawn game"                            
                            else null 
                        end as opening,
                        players_black_user_name,
                        players_black_user_title,
                        players_white_user_name,
                        players_white_user_title,
                        winner
                    from lichess_main

                    where (players_black_user_title = "GM" and players_white_user_title = "GM")

        """)

    moves_table.show()

    moves_table.write.mode('append').parquet("output_data/" + "moves/")

def flatten_json(json_responses):

    full_flattened_json = []

    for response in json_responses:

        flattened_json = flatten(response)

        print(flattened_json)

        full_flattened_json.append(flattened_json)   

    return full_flattened_json

def get_lichess_games(player_list):

    for player in player_list:

        load_json_files_to_spark("https://lichess.org/api/games/user/" + player, 10)


def main():

    players = ["alireza2003", "Konevlad", "neslraCsungaM77", "Vladimirovich9000", "sp1cycaterpillar", "Federicov93", "may6enexttime", "Kelevra317", "nihalsarin2004", " Drvitman", "DrNykterstein", "C9C9C9C9C9", "muisback", "Inventing_Invention", "RebeccaHarris", "drop_stone", "Alexander_Zubov", "IWANNABEADOORED", "Kelevra317", "dolar9", "cutemouse83"]

    get_lichess_games(players)

if __name__ == "__main__":
    main()
