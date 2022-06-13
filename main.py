from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import src.finding_answers as finder
from datetime import datetime

spark = SparkSession.builder.master("local[*]").appName("lab9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


if __name__ == "__main__":
    categories_df = spark.read.format("json") \
        .option("multiline", "true") \
        .load("./data/GB_category_id.json")

    categories_df = (
        categories_df
        .withColumn("categories_exploaded", f.explode(f.arrays_zip("items.id", "items.snippet.title")))
        .select(
            f.col("categories_exploaded.id").alias("id"),
            f.col("categories_exploaded.title").alias("title"),
        )
    )

    # Read videos DF.
    videos_df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", ",") \
        .option("multiline", True) \
        .load("./data/GBvideos.csv")

    videos_df.createGlobalTempView("video")
    categories_df.createGlobalTempView("categories")
    categories_df.show()

    #1 Query
    #finder.top_trending_videos(videos_df, spark)

    # 2 Query
    #finder.top_week_categories(videos_df, categories_df, spark)

    # 3 Query
    #finder.top_tags_monthly(videos_df, spark)

    # 4 Query
    #finder.top_channels(videos_df, spark)

    # 5 Query
    #finder.top_trending_channels(videos_df, spark)

    # 6 Query
    #finder.top_category_videos(videos_df, categories_df, spark)

