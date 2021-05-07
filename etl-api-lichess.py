from pathlib import Path
import requests, json
from datetime import datetime
import os
from flatten_json import flatten
import asyncio
import yaml
import argparse
import pandas as pd
import s3fs

async def load_json_files_to_staging(url, nbgames, config, local):

    """
    Fetches data from the Lichess URL passed to it
    and saves it as a parquet file.

    Calls  flatten_json() to flatten the nested JSON
    received in the API response from Chess.com
    
    Params: 
    url = the Chedss.com API URL that is fetched
    config = the config object (the contents of the yaml file)    
    nbgames: the number of games to get from each player (passed in
    from config file)
    local: Whether the file should be saved locally (True or False)
    config: the config object (the contents of the yaml file)
    
    Returns:
    None
    """

    if local == True:
        output_data = config['output_data_path_local']
    else:
        output_data = "s3://" + config['output_data_path_s3']

    params = {'opening': 'true', 'max': nbgames}

    headers = {'Accept': 'application/x-ndjson'}

    response = requests.get(url, headers=headers, params=params).text

    print("Fetched data from " + url)

    json_lines = response.splitlines()

    json_data = []

    pd_df = pd.DataFrame([])

    for line in json_lines:
        json_data.append(json.loads(line))
 
    flattened_json_responses = flatten_json(json_data)

    try:

        pd_df = pd_df.append(flattened_json_responses)
              
    
        url_split = url.split("/")

        # Write API response to parquet files in `raw` dir

        write_path = output_data + "raw/lichess2/" + url_split[6] + '.parquet'

        print(f"Writing parquet file to path: {write_path}")    
        pd_df.to_parquet(write_path)


    except Exception as error:
        print(f"An exception occurred {error}")


def flatten_json(json_responses):

    """
    Receives a list of dicts with nested items 
    and flattens them using the flatten_json.flatten(), 
    before returning them as a list of flattened dicts.
    
    Params: 
    json_responses: The list of dicts
    
    Returns:
    full_flattened_json: A list of flattened json dicts
    """

    full_flattened_json = []

    for response in json_responses:

        flattened_json = flatten(response)

        full_flattened_json.append(flattened_json)   

    return full_flattened_json


async def get_lichess_games(player_list, nbgames, config, local):

    """
    Gets the archive URLs of all games played by
    a player and then recursively calls load_json_files_to_staging()
    on all those player archive URLs.
    
    Params: 
    player_list: list of player usernames (passed in from config file)
    nbgames: the number of games to get from each player (passed in
    from config file)
    local: Whether the file should be saved locally (True or False)
    config: the config object (the contents of the yaml file)
    
    Returns:
    None
    """

    for player in player_list:

        await asyncio.gather(load_json_files_to_staging("https://lichess.org/api/games/user/" + player, nbgames, config, local))


async def main():

    """
    The main function of the ETL API Lichess script. 

    Gets the list of players from the yaml file and
    reads command line  arguments via argparse, to see
    whether the user wants save the data locally or on s3.

    Thereafter, calls get_lichess_games() via asyncio.
    
    Params: 
    None
    
    Returns:
    None
    """

    #Load settings from yaml file

    with open(r'dl-lichess.yaml') as file:
        config = yaml.load(file)

     # Sets AWS access environment variables.
    
    os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']


    # Grab CLI args from argparse.

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

        await asyncio.gather(get_lichess_games(players, nbgames, config, True))
    else:
        await asyncio.gather(get_lichess_games(players, nbgames, config, False))

if __name__ == "__main__":
    asyncio.run(main())
