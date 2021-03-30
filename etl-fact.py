from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext


spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()


df = spark.read.parquet("output_data/staging/*")

df.createOrReplaceTempView("staging")

df_csv = spark.read.option("header",True).csv("output_data/chess_openings.csv")


df_csv.createOrReplaceTempView("staging_openings")

moves_table = spark.sql("""

                        select
                            from_unixtime(createdAt/1000, 'dd-MM-yyyy hh:mm:ss') as game_start,
                            from_unixtime(lastMoveAt/1000, 'dd-MM-yyyy hh:mm:ss') as game_end,
                            lastMoveAt/1000 - createdAt/1000 as game_length_seconds,
                            id,
                            speed,
                            staging.moves,
                            staging.opening_name,
                            staging.opening_ply,
                            staging_openings.name as openings_csv,
                            players_black_user_name as black_player_name,
                            players_black_user_title as black_player_title,
                            players_black_rating as black_player_rating,
                            players_white_user_name as white_player_name,
                            players_white_user_title as white_player_title,
                            players_white_rating as white_player_rating,
                            status,
                            winner
                        from staging

                        left join staging_openings on staging.moves LIKE CONCAT('%', substr(staging_openings.moves,0,8) ,'%')
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