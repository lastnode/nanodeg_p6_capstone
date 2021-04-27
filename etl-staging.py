from pathlib import Path
import requests, json
from datetime import datetime
import os
from flatten_json import flatten
import asyncio
import configparser
import yaml
import argparse
import pandas as pd

with open(r'dl-lichess.yaml') as file:
    config = yaml.load(file)

os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']


async def load_json_files_to_staging(url, nbgames, local):

    if local == True:
        output_data = config['output_data_path_local']
    else:
        output_data = config['output_data_path_s3']

    params = {'opening': 'true'}

    headers = {'Accept': 'application/x-ndjson'}

    response = requests.get(url, headers=headers, params=params).text

    json_lines = response.splitlines()

    json_data = []

    pd_df = pd.DataFrame([])


    for line in json_lines:
        json_data.append(json.loads(line))
 
    flattened_json_responses = flatten_json(json_data)

    try:
        # df = spark.createDataFrame(flattened_json_responses)

        pd_df = pd_df.append(flattened_json_responses)
        
        # df.show()
        
        # df.createOrReplaceTempView("staging")

        #df.write.mode('append').parquet(output_data + "staging/lichess_local_v1")

    except ValueError:
        pass
    
    url_split = url.split("/")

    pd_df.to_parquet(output_data + "staging/lichess_local_v1/" + url_split[6] + '.parquet')


def flatten_json(json_responses):

    full_flattened_json = []

    for response in json_responses:

        flattened_json = flatten(response)

        print(flattened_json)

        full_flattened_json.append(flattened_json)   

    return full_flattened_json


async def get_lichess_games(player_list, nbgames, local):

    for player in player_list:

        await asyncio.gather(load_json_files_to_staging("https://lichess.org/api/games/user/" + player, nbgames, local))


async def main():

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

    players = config['lichess_players']

    print(players)

    nbgames = config['max_games_per_player']

    if args.local:
        print("Saving outputd data locally instead of writing to s3 bucket.")

        await asyncio.gather(get_lichess_games(players, nbgames, True))
    else:
        await asyncio.gather(get_lichess_games(players, nbgames, False))

if __name__ == "__main__":
    asyncio.run(main())
