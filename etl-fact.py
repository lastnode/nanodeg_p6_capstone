from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SQLContext


spark = SparkSession \
    .builder \
    .appName("Ingesting Lichess API via Spark") \
    .getOrCreate()


df = spark.read.parquet("output_data/staging/*")

df.createOrReplaceTempView("staging")

openings_table = spark.sql("""


                    select
                    	opening_name,
                    	count(*) as games
                    from staging

                    group by opening_name
                    order by games
				
				""")

openings_table.write.mode('append').parquet("output_data/fact/" + "openings/")