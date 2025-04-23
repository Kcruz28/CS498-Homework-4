from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder \
    .appName("Preprocess Data") \
    .getOrCreate()

df = spark.read.text("searchLog.csv")

def parsing_function(row):
    output = []
    term, url_and_clicks = row.value.split(":", 1)
    pairing = url_and_clicks.split("~")
    for pair in pairing:
        url, clicks = pair.split(":")
        output.append(Row(term=term, url=url, clicks=int(clicks)))
    return output

parsed_rdd = df.rdd.flatMap(parsing_function)
parsed_df = spark.createDataFrame(parsed_rdd)

parsed_df.repartition(parsed_df.count()).write.json("processed_data", mode="overwrite")

spark.stop()
