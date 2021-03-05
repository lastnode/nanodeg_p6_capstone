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


def load_json_files_to_spark(url):

    spark = SparkSession \
    .builder \
    .appName("DataCleansing") \
    .getOrCreate()

    params = {'max':'100'}

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
                        createdAt,
                        id,
                        moves,
                        players_black_user_name,
                        players_white_user_name,
                        winner
                    from lichess_main



        """)

    moves_table.show()

    moves_table.write.mode('append').parquet("output_data/" + "moves/")

def flatten_json(json_responses):

    full_flattened_json = []

    for response in json_responses:

        flattened_json = flatten(response)

        print(type(flattened_json))

        full_flattened_json.append(flattened_json)   

    return full_flattened_json


def main():


    load_json_files_to_spark("https://lichess.org/api/games/user/alireza2003")
    load_json_files_to_spark("https://lichess.org/api/games/user/GothamChess")



if __name__ == "__main__":
    main()
