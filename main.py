from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime
import json

spark = SparkSession.builder.master("local[*]").appName("lab9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def process_date(given_date):
    """
    year.day.month =>  datetime(year, month, day)
    """
    date_split = given_date.split(".")
    try:
        return datetime(int(date_split[0]), int(date_split[2]), int(date_split[1]))
    except:
        return None


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
    trend_videos = spark.sql(
        "SELECT video_id as id, title, description, COUNT(*) as num_days_tranded FROM global_temp.video GROUP BY video_id, title, description ORDER BY COUNT(*) DESC LIMIT 10;")
    trend_videos.createGlobalTempView("trend_videos")

    video_small = spark.sql(
        "SELECT id, trend_videos.title, trend_videos.description, trending_date, views, likes, dislikes FROM global_temp.trend_videos INNER JOIN global_temp.video ON video_id == id ORDER BY id, num_days_tranded DESC;")
    video_small.createGlobalTempView("video_small")

    top_trend_videos = spark.sql("SELECT DISTINCT id, title, description FROM global_temp.video_small;")
    top_trend_videos.createGlobalTempView("top_trend_videos")

    min_data = spark.sql(
        "SELECT id, MIN(trending_date) as data FROM global_temp.video_small GROUP BY id;")

    min_data_row = min_data.collect()
    min_data_str = [[ele.__getattr__("id"), ele.__getattr__("data")] for ele in min_data_row]

    trending_days = {tr_id[0]: video_small.where(video_small["id"] == tr_id[0])
    .select("trending_date", "views", "likes", "dislikes")
    .rdd.map(lambda r: {"date": r[0], "views": r[1], "likes": r[2], "dislikes": r[3]}).collect()
                     for tr_id in min_data_str}

    latest_days = dict()
    i = 0
    for current_id, data in trending_days.items():
        for day in data:
            if day["date"] == min_data_str[i][1]:
                latest_days[current_id] = {"views": day["views"],
                                           "likes": day["likes"],
                                           "dislikes": day["dislikes"]}
        i += 1

    top_trend_videos.show()
    answer_1 = {"videos": []}
    for current_video in top_trend_videos.collect():
        answer_1["videos"].append({"id": current_video["id"],
                                   "title": current_video["title"],
                                   "description": current_video["description"],
                                   "latest_views": latest_days[current_video["id"]]["views"],
                                   "latest_likes": latest_days[current_video["id"]]["likes"],
                                   "latest_dislikes": latest_days[current_video["id"]]["dislikes"],
                                   "trending_days": trending_days[current_video["id"]]
                                   })

    json_object = json.dumps(answer_1)
    with open("results/answer1.json", "w") as outfile:
        outfile.write(json_object)

    # =================== TASK 1 ===================
    # top_trending_videos_df = find.top_trending_videos(videos_df, limit=10)
    # print("=" * 12, "TASK 1", "=" * 12)
    # top_trending_videos_df.printSchema()
    # top_trending_videos_df.show()
    #
    # # =================== TASK 2 ===================
    # week_top_categories_df = find.top_week_categories(videos_df, categories_df)
    # print("=" * 12, "TASK 2", "=" * 12)
    # week_top_categories_df.printSchema()
    # week_top_categories_df.show()
    #
    # # =================== TASK 3 ===================
    # top_tags_monthly_df = find.top_tags_monthly(videos_df)
    # print("=" * 12, "TASK 3", "=" * 12)
    # top_tags_monthly_df.printSchema()
    # top_tags_monthly_df.show()
    #
    # # =================== TASK 4 ===================
    # top_channels_df = find.top_channels(videos_df)
    # print("=" * 12, "TASK 4", "=" * 12)
    # top_channels_df.printSchema()
    # top_channels_df.show()
    #
    # # =================== TASK 5 ===================
    # top_trending_channels_df = find.top_trending_channels(videos_df)
    # print("=" * 12, "TASK 5", "=" * 12)
    # top_trending_channels_df.printSchema()
    # top_trending_channels_df.show()
    #
    # # =================== TASK 6 ===================
    # top_category_videos_df = find.top_category_videos(videos_df, categories_df)
    # print("=" * 12, "TASK 6", "=" * 12)
    # top_category_videos_df.printSchema()
    # top_category_videos_df.show()
