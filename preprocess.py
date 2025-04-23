from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("Preprocess Data").getOrCreate()

df = spark.read.text("searchLog.csv")


def parsing_function(row):
    output = []
    header_and_rest = row.value.split(": ", 1)
    if len(header_and_rest) != 2:
        return []
    term_and_urls = header_and_rest[1].split(",", 1)
    if len(term_and_urls) != 2:
        return []
    term = term_and_urls[0].strip().strip("‘’'\"")
    url_list = term_and_urls[1]
    pairing = url_list.split("~")
    for pair in pairing:
        url, clicks = pair.split(":")
        output.append(Row(term=term, url=url, clicks=int(clicks)))
    return output


parsed_rdd = df.rdd.flatMap(parsing_function)
parsed_df = spark.createDataFrame(parsed_rdd)

parsed_df.repartition(parsed_df.count()).write.json("processed_data", mode="overwrite")


spark.stop()
