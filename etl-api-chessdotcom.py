import pandas as pd
from datetime import datetime
import yaml, argparse, os, requests, json
from chessdotcom import get_player_games_by_month, get_player_game_archives
import s3fs


def load_json_files_to_staging(url, local, config):

    """
    Fetches data from the Chess.com URL passed to it
    and savesit as a parquet file.
    
    Params: 
    url = the Chedss.com API URL that is fetched
    local = Whether the file should be saved locally (True or False)
    config = the config object (the contents of the yaml file)
    
    Returns:
    None

    """

    print("Fetching data from: " + url)

    if local == True:
        output_data = config['output_data_path_local']
    else:
        output_data = "s3://" + config['output_data_path_s3']

    response = requests.get(url).text

    games = json.loads(response)

    games = games.pop('games')

    pd_df = pd.DataFrame([])

    for game in games:

        try:

            flattened_json = pd.json_normalize(game, sep="_")

            pd_df = pd_df.append(flattened_json)

        except:
            pass
           
    try:        
        url_split = url.split("/")

        # Write API response to parquet files in `raw` dir

        write_path = output_data + "raw/chessdotcom2/" + url_split[5] + "_" + url_split[7] + "_" + url_split[8] + '.parquet'
        
        print(f"Writing parquet file to path: {write_path}")    
        pd_df.to_parquet(write_path)

    except Exception as error:
        print(f"An exception occurred {error}")

def get_chesscom_games(player_list, local, config):

    """
    Gets the monthly archive URLs of all games played by
    a player and then recursively calls load_json_files_to_staging()
    on all those monthly archive URLs.
    
    Params: 
    player_list = list of player usernames (passed in from config file)
    local = Whether the file should be saved locally (True or False)
    config = the config object (the contents of the yaml file)
    
    Returns:
    None
    """

    for player in player_list:

        player_archives = get_player_game_archives(player)

        for urls in player_archives.json.values():

            for url in urls:

                load_json_files_to_staging(url, local, config)


def main():

    """
    The main function of the ETL API Chess.com script. 

    Gets the list of players from the yaml file and
    reads command line  arguments via argparse, to see
    whether the user wants save the data locally or on s3.

    Thereafter, calls get_chesscom_games().
    
    Params: 
    None
    
    Returns:
    None
    """

    #Load settings from yaml file


    with open(r'config/dl-chessdotcom.yaml') as file:
        config = yaml.load(file)

    # Sets AWS access environment variables.

    os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']

    # Grab CLI args from argparse.

    parser = argparse.ArgumentParser(
        prog='etl-staging.py',
        description="""ETL Script that extracts data from
            Chess.com API and saves them in parquet files.""")

    parser.add_argument(
        '-l', '--local',
        action='store_true',
        help="""Save data locally instead of outputting to s3.""")

    args, _ = parser.parse_known_args()

    players = config['chessdotcom_players']

    print(players)

    if args.local:
        print("Saving outputd data locally instead of writing to s3 bucket.")

        get_chesscom_games(players, True, config)
    else:
        get_chesscom_games(players, False, config)

if __name__ == "__main__":
    main()
