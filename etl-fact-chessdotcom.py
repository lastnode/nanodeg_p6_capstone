from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SQLContext
import argparse
import yaml
import os
from pgn_parser import parser, pgn

def get_eco_from_pgn(pgn_text):

    """
    Receives the `pgn` column from Spark SQL and uses the
    `pgn_parser` module to extract  the `ECOUrl` field from
    the PGN string, before doing a string replace on it to 
    remove dashes and return the full name of the ECO.
    
    Parameters:
    - pgn_text - The text of the `pgn` column, as passed in by Spark SQL.
 
    Returns:
    - eco_url_replaced - The ECO name, as extracted from the ECOUrl field
    of the PGN text.
    """

    try:
        game = parser.parse(pgn_text, actions=pgn.Actions())
    
        eco_url = game.tag_pairs["ECOUrl"]

        eco_url_split = eco_url.split("/")

        eco_url_replaced = eco_url_split[-1].replace("-"," ") # Return the last element of the list (the part after the last / character)

        return eco_url_replaced 

    except:
            pass


def get_termination_from_pgn(pgn_text):

    """
    Receives the `pgn` column from Spark SQL and uses the
    `pgn_parser` module to extract  the `Termination` field from
    the PGN string and return it.
    
    Parameters:
    - pgn_text - The text of the `pgn` column, as passed in by Spark SQL.
 
    Returns:
    - game.tag_pairs["Termination"] - The `Termination` field,
     as extracted from the Termination field  of the PGN text.
    """

    try:
        game = parser.parse(pgn_text, actions=pgn.Actions())
    
        return game.tag_pairs["Termination"]

    except:
            pass


def get_moves_from_pgn(pgn_text):

    """
    Receives the `pgn` column from Spark SQL and uses the
    `pgn_parser` module to extract the game moves from
    the PGN string and return it.
    
    Parameters:
    - pgn_text - The text of the `pgn` column, as passed in by Spark SQL.
 
    Returns:
    - game.movetext - The moves of the game, as extracted from the
    Termination field  of the PGN text.
    """

    try: 
        game = parser.parse(pgn_text, actions=pgn.Actions())
    
        return str(game.movetext)

    except:
            pass


def main():

    """
    The main function of the ETL script. Reads command line
    arguments via argparse, to see whether the user wants to
    use local data or s3 data.

    Creates a Spark Session, sets Hadoop s3a settings, and then
    reads the `*.parquet` files from the `raw/chessdotcom/` dir.

    Then, it calls the following PySpark UDFs within Spark SQL
    to transform the data:

    1) get_eco_from_pgn()
    2) get_termination_from_pgn()
    2) get_moves_from_pgn()

    Finally, it writes the output table to `staging/chessdotcom/games`.
    
    Params: 
    None
    Returns:
    None
    """

    with open(r'dl-chesscom.yaml') as file:
        config = yaml.load(file,Loader=yaml.SafeLoader)

    os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']

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

    # If --local CLI flag is set, use local path.
    # Else use S3 path

    if args.local:
        output_data = config['output_data_path_local']
    else:
        output_data = config['output_data_path_s3']

    # Create SparkSession
    spark = SparkSession \
        .builder \
        .appName("Ingesting Chess.com API via Spark") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')\
        .getOrCreate()

    # Setting the MapReduce algorithm to v2, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 

    # Setting s3a configs
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",config['aws_access_key_id'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",config['aws_secret_key_id'])

    # Register PySpark UDFs - https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/
    spark.udf.register("get_eco_from_pgn", get_eco_from_pgn)
    spark.udf.register("get_termination_from_pgn", get_termination_from_pgn)
    spark.udf.register("get_moves_from_pgn", get_moves_from_pgn)

    df = spark.read.parquet(output_data + "raw/chessdotcom/g*")

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
