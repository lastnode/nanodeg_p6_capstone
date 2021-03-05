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
                        case 
                            when moves like "%e4 c5%" then "B20 Sicilian defence"
                            when moves like "%e4 c5 f4%" then "B21 Sicilian, Grand Prix attack"
                            when moves like "%e4 c5 Nf3%" then "B27 Sicilian defence"
                            when moves like "%d4 d5 c4 dxc4%" then "D20 Queen's gambit accepted"
                            when moves like "%d4 d5 c4 dxc4 Nf3%" then "D21 Queen's Gambit Accepted, 3.Nf3"                            
                            when moves like "%d4 d5 c4 dxc4 Nf3 a6%" then "D22 Queen's Gambit Accepted, Alekhine defence"                            
                            else null 
                        end as opening,
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
