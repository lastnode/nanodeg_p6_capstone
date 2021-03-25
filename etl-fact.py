from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext


spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()


df = spark.read.parquet("output_data/staging/*")

df.createOrReplaceTempView("staging")

moves_table = spark.sql("""

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
                        from staging
            """)

moves_table.show()

moves_table.write.mode('append').parquet("output_data/fact/" + "moves/")

openings_table = spark.sql("""


                    select
                    	opening_name,
                    	count(*) as games
                    from staging

                    group by opening_name
                    order by games
				
				""")

openings_table.write.mode('append').parquet("output_data/fact/" + "openings/")