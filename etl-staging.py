from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pathlib import Path
import requests, json
from datetime import datetime
import os
from flatten_json import flatten
import asyncio

async def load_json_files_to_staging(url, nbgames):

    spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
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
        
        df.show()

        df.createOrReplaceTempView("staging")

        df.write.mode('append').parquet("output_data/" + "staging/")

    except ValueError:
        pass
        

def flatten_json(json_responses):

    full_flattened_json = []

    for response in json_responses:

        flattened_json = flatten(response)

        print(flattened_json)

        full_flattened_json.append(flattened_json)   

    return full_flattened_json


async def get_lichess_games(player_list, nbgames):

    for player in player_list:

        await asyncio.gather(load_json_files_to_staging("https://lichess.org/api/games/user/" + player, nbgames))


async def main():

    players = ["alireza2003", "Konevlad", "neslraCsungaM77", "Vladimirovich9000", "sp1cycaterpillar", "Federicov93", "may6enexttime", "Kelevra317", "nihalsarin2004", " Drvitman", "DrNykterstein", "C9C9C9C9C9", "muisback", "Inventing_Invention", "RebeccaHarris", "drop_stone", "Alexander_Zubov", "IWANNABEADOORED", "Kelevra317", "dolar9", "cutemouse83"]

    nbgames = 1000

    await asyncio.gather(get_lichess_games(players, nbgames))


if __name__ == "__main__":
    asyncio.run(main())
