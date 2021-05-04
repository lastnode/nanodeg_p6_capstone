from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SQLContext
import argparse
import yaml
import os
from pgn_parser import parser, pgn
from sql_queries import games_table_chessdotcom,games_table_lichess
from sys import exit

def get_eco_from_pgn(pgn_text):

    """
    Only used for Chess.com transformations.

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
    Only used for Chess.com transformations.

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
    Only used for Chess.com transformations.

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

    #Load settings from yaml file

    with open(r'dl-chesscom.yaml') as file:
        config = yaml.load(file,Loader=yaml.SafeLoader)

    # Sets AWS access environment variables, even though
    # this is strictly not necessary now, since we use
    # `SimpleAWSCredentialsProvider` as the cred provider.

    os.environ['AWS_ACCESS_KEY_ID']=config['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_secret_key_id']

    # Grab CLI args from argparse.

    parser = argparse.ArgumentParser(
    prog='etl-staging.py',
    description="""ETL Script that extracts data from
        raw parquet files extracted from the Chess.com / Lichess 
        APIs and loads them into a staging table in parquet files.""")

    parser.add_argument(
        '-l', '--local',
        action='store_true',
        help="""Save data locally instead of outputting to s3.""")

    parser.add_argument('--platform',
                        dest='platform',
                        const="chessdotcom",
                        type=str,
                        nargs='?',
                        help='Transform Chess.com or Lichess raw. data')

    args, _ = parser.parse_known_args()   

    # If --local CLI flag is set, use local path.
    # Else use S3 path

    if args.local:
        output_data = config['output_data_path_local']
    else:
        output_data = "s3a://" + config['output_data_path_s3']


    # Read --platform= CLI flag to see which platform
    # we are performing the transformations on.

    if args.platform == "chessdotcom":
        
        user_selected_platform = "chessdotcom"
    
    elif args.platform == "lichess":

        user_selected_platform = "lichess"

    else:
        print("Error:\n No platform selected. Please set either '--platform=chessdotcom' or '--platform=lichess'")
        exit()

    # Create the Spark session
    spark = create_spark_session(config, user_selected_platform)

    process_data(spark, output_data, user_selected_platform)

def create_spark_session(config, platform):

    """
    Creates a Spark Session and returns it
    so that other functions can use it.

    Paramters:
    None
    
    Returns:
    A Spark Session Object
    """

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
    # But only if the platform is Chess.com, as we don't use these functions for Lichess.
    
    if platform == "chessdotcom":

        spark.udf.register("get_eco_from_pgn", get_eco_from_pgn)
        spark.udf.register("get_termination_from_pgn", get_termination_from_pgn)
        spark.udf.register("get_moves_from_pgn", get_moves_from_pgn)

    return spark

def process_data(spark, output_data, platform):

    try:
        # Read from raw data parquet files and create df

        if platform == "chessdotcom":
        
            df = spark.read.parquet(output_data + "raw/chessdotcom/*.parquet")

            df.createOrReplaceTempView("chessdotcom_staging")

            games_table = spark.sql(games_table_chessdotcom)

        elif platform == "lichess":

            df = spark.read.parquet(output_data + "raw/lichess/*.parquet")

            df.createOrReplaceTempView("lichess_staging")

            games_table = spark.sql(games_table_lichess)
    
        # Write transformed table to parquet files in staging dir

        if platform == "chessdotcom":

            games_table.write.mode('append').parquet(output_data +"staging/chessdotcom/" + "games2/")

        elif platform == "lichess":

            games_table.write.mode('append').parquet(output_data +"staging/lichess/" + "games2/")

    except Exception as error:
        print(f"An exception occurred {error}")

if __name__ == "__main__":
    main()
