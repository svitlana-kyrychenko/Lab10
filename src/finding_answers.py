from pyspark.sql import types as t
from pyspark.sql import functions as f
from datetime import timedelta, datetime
import json

def top_trending_videos(videos_df, spark):
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
    print("1 Query END ////////////////////////////////////////////////////////////////////\n\n\n")
    with open("results/answer1.json", "w") as outfile:
        outfile.write(json_object)


# ======================== TASK 2 ========================

week_boundaries_schema = t.StructType([
    t.StructField("start_date", t.DateType()),
    t.StructField("end_date", t.DateType()),
    t.StructField("week_num", t.StringType())
])


@f.udf(week_boundaries_schema)
def get_week_boundaries(date):
    date_dt = datetime.strptime(str(date), "%y.%d.%m")
    start = date_dt - timedelta(days=date_dt.weekday())
    end = start + timedelta(days=6)
    week_num = date_dt.strftime("%y%W")
    return start, end, week_num

def get_videos_by_category(category, start_date, end_date, spark):
    videos_by_category = spark.sql(
        "SELECT DISTINCT video_id FROM global_temp.video WHERE category_id = \"" + category + "\"  AND trending_date >= "+ start_date+ " AND trending_date <= " + end_date +";")
    videos_by_category_row = videos_by_category.collect()

    return [[ele.__getattr__("video_id")] for ele in videos_by_category_row]

def top_week_categories(videos_df, categories_df, spark):
    weeks_df = spark.sql(
        "SELECT video_id as id, trending_date as date_video, views, category_id FROM global_temp.video;")

    weeks_df.show()

    weeks_video = weeks_df \
        .withColumn("week_boundaries", get_week_boundaries(f.col("date_video"))) \
        .select(
        f.col("id"),
        f.col("views"),
        f.col("date_video"),
        f.col("category_id"),
        f.col("week_boundaries.start_date").alias("start_date"),
        f.col("week_boundaries.end_date").alias("end_date"),
        f.col("week_boundaries.week_num").alias("week_num")
    )

    weeks_video.show()

    weeks_video.createGlobalTempView("video_week")

    video_first_income = spark.sql(
        "SELECT id, MIN(date_video) as date_video_min, week_num FROM global_temp.video_week GROUP BY id, week_num;")

    video_first_income.show()

    video_first_income.createGlobalTempView("video_first_income")

    weeks_video_new = spark.sql(
        "SELECT * FROM global_temp.video_week as x WHERE NOT EXISTS (SELECT * FROM global_temp.video_first_income as min_video WHERE min_video.id = x.id  AND min_video.date_video_min = x.date_video);")

    weeks_video_new.show()
    weeks_video_new.createGlobalTempView("weeks_video_new")

    week_category_popular = spark.sql(
        "SELECT week_num, category_id, SUM(views) as num_of_views, COUNT(DISTINCT id) as num_videos FROM global_temp.weeks_video_new GROUP BY week_num, category_id;")
    week_category_popular.show()
    week_category_popular.createGlobalTempView("week_category_popular")

    week_popular = spark.sql(
        "SELECT week_num, MAX(num_of_views) as max_num_of_views FROM global_temp.week_category_popular GROUP BY week_num;")
    week_popular.show()
    week_popular.createGlobalTempView("week_popular")

    week_category = spark.sql(
        "SELECT * FROM global_temp.week_category_popular as x WHERE EXISTS(SELECT * FROM global_temp.week_popular AS y WHERE x.num_of_views = y.max_num_of_views AND x.week_num = y.week_num);")
    week_category.show()
    week_category.createGlobalTempView("week_category")

    week_category_with_start = spark.sql(
        "SELECT week_category.week_num, week_category.category_id, week_category.num_of_views, num_videos, start_date, end_date FROM global_temp.week_category INNER JOIN global_temp.weeks_video_new ON week_category.week_num = weeks_video_new.week_num AND week_category.category_id = weeks_video_new.category_id;")
    week_category_with_start.show()
    week_category_with_start.createGlobalTempView("week_category_with_start")

    result = spark.sql(
        "SELECT week_num, week_category_with_start.category_id, title, num_of_views, num_videos, start_date, end_date FROM global_temp.week_category_with_start INNER JOIN global_temp.categories ON  week_category_with_start.category_id = categories.id;")
    result.show()

    result_row = result.collect()
    weeks = [[{"start_date": ele.__getattr__("start_date"), "end_date": ele.__getattr__("end_date"),
                      "category_id": ele.__getattr__("category_id"), "category_name": ele.__getattr__("title"),
                      "number_of_videos": ele.__getattr__("num_videos"), "total_views": ele.__getattr__("num_of_views"),
                      "video_ids": get_videos_by_category(ele.__getattr__("category_id"), ele.__getattr__("start_date"), ele.__getattr__("end_date"), spark)}] for ele in result_row]
    print("here")
    answer_2 = {"weeks": weeks}

    json_object = json.dumps(answer_2)

    print("2 Query END ////////////////////////////////////////////////////////////////////\n\n\n")
    with open("results/answer2.json", "w") as outfile:
        outfile.write(json_object)


# ======================== TASK 3 ========================

date_ranges_schema = t.StructType([
    t.StructField("start_date", t.DateType()),
    t.StructField("end_date", t.DateType()),
    t.StructField("period_num", t.StringType())
])


@f.udf(date_ranges_schema)
def date_ranges_udf(date):
    date_days = int(date.strftime("%-j"))
    period_id = int(date_days / 30)
    delta_left = timedelta(days=date_days - period_id * 30)
    delta_right = timedelta(days=((period_id + 1) * 30) - date_days)
    period_num = str(period_id)+date.strftime("%y")
    start_date = date - delta_left
    end_date = date + delta_right

    return start_date, end_date, period_num


def top_tags_monthly(videos_df, spark):
    tags_exploaded_df = (
        videos_df
        .select(
            f.col("video_id"),
            f.split(f.regexp_replace(f.col("tags"), '\"', ''), "\|").alias("tags_array"),
            f.to_date(f.from_unixtime(f.unix_timestamp(f.col("trending_date"), "yy.dd.MM"))).alias("date")
        )
        .withColumn("tag", f.explode(f.col("tags_array")))
        .select(
            f.col("video_id"),
            f.col("tag"),
            f.col("date")
        ).distinct()
    )
    tags_exploaded_df.show()

    tags_time_periods = (
        tags_exploaded_df
        .withColumn("start_end_dates", date_ranges_udf(f.col("date")))
        .select(
            f.col("video_id"),
            f.col("tag"),
            f.col("date"),
            f.col("start_end_dates.start_date").alias("start_date"),
            f.col("start_end_dates.end_date").alias("end_date"),
            f.col("start_end_dates.period_num").alias("period_num")
        )
    )
    tags_time_periods.createGlobalTempView("period_tag")
    tags_time_periods.show()

    max_video_days = spark.sql(
         "SELECT  video_id, period_num, MAX(date)  FROM global_temp.period_tag GROUP BY period_num, video_id;")
    max_video_days.createGlobalTempView("max_video_days")
    max_video_days.show()

    tags_time_periods_last = spark.sql(
         "SELECT * FROM global_temp.period_tag as x WHERE EXISTS(SELECT * FROM global_temp.max_video_days as y WHERE x.video_id = y.video_id AND x.period_num = y.period_num);")
    tags_time_periods_last.show()
    tags_time_periods_last.createGlobalTempView("tags_time_periods_last")

    tags_popular = spark.sql(
        "SELECT tag, period_num, COUNT(*) as num_videos FROM global_temp.tags_time_periods_last GROUP BY tag, period_num;")
    tags_popular.show()
    tags_popular.createGlobalTempView("tags_popular")

    limited_popular = spark.sql(
        "SELECT * FROM (SELECT *, row_number() over(partition by tag, period_num order by num_videos desc) as seqnum FROM global_temp.tags_popular) WHERE seqnum <= 10;")
    limited_popular.show()
    limited_popular.createGlobalTempView("limited_popular")

    result = spark.sql(
         "SELECT DISTINCT limited_popular.tag, limited_popular.period_num, start_date, end_date, num_videos FROM global_temp.tags_time_periods_last INNER JOIN global_temp.limited_popular ON tags_time_periods_last.period_num = limited_popular.period_num;")
    result.show()
    result.createGlobalTempView("result")

    print("3 Query END ////////////////////////////////////////////////////////////////////\n\n\n")


def top_channels(videos_df, spark):
    max_video_chanel_view = spark.sql(
        "SELECT channel_title, video_id, MAX(views) as num_views FROM global_temp.video GROUP BY channel_title, video_id;")
    max_video_chanel_view.show()
    max_video_chanel_view.createGlobalTempView("max_video_chanel_view")

    chanel_boundaries_join = spark.sql(
        "SELECT max_video_chanel_view.channel_title, video.video_id, trending_date FROM global_temp.max_video_chanel_view INNER JOIN global_temp.video ON video.video_id = max_video_chanel_view.video_id;")
    chanel_boundaries_join.show()
    chanel_boundaries_join.createGlobalTempView("chanel_boundaries_join")

    chanel_boundaries = spark.sql(
        "SELECT channel_title, MIN(trending_date) as start_date, MAX(trending_date) as end_date FROM global_temp.chanel_boundaries_join GROUP BY channel_title;")
    chanel_boundaries.show()
    chanel_boundaries.createGlobalTempView("chanel_boundaries")

    sum_chanel_views = spark.sql(
        "SELECT channel_title, SUM(num_views) as views, COUNT(*) as num_videos FROM global_temp.max_video_chanel_view GROUP BY channel_title ORDER BY views LIMIT 20;")
    sum_chanel_views.show()
    sum_chanel_views.createGlobalTempView("sum_chanel_views")

    print("4 Query END ////////////////////////////////////////////////////////////////////\n\n\n")


def top_trending_channels(videos_df, spark):
    max_trending_days = spark.sql(
        "SELECT channel_title, COUNT(*) as num_views FROM global_temp.video GROUP BY channel_title LIMIT 10;")
    max_trending_days.show()
    max_trending_days.createGlobalTempView("max_chanel_view")

    print("5 Query END ////////////////////////////////////////////////////////////////////\n\n\n")


def top_category_videos(videos_df, categories_df, spark):
    max_video_view = spark.sql(
        "SELECT * as num_views FROM global_temp.video WHERE views >= 100000;")
    max_video_view.show()
    max_video_view.createGlobalTempView("max_video_view")

    videos_ratio = (
        max_video_view
        .select(
            f.col("video_id"),
            f.col("title").alias("video_title"),
            f.col("views").cast(t.LongType()).alias("views"),
            f.col("likes").cast(t.LongType()).alias("likes"),
            f.col("dislikes").cast(t.LongType()).alias("dislikes"),
            f.col("category_id")
        )
        .withColumn("ratio_likes_dislikes", f.col("likes") / f.col("dislikes"))
        .select(
            f.col("category_id"),
            f.col("video_id"),
            f.col("video_title"),
            f.col("views"),
            f.col("ratio_likes_dislikes")
        )
    )
    videos_ratio.show()
    videos_ratio.createGlobalTempView("videos_ratio")

    videos_ratio_best = spark.sql(
        "SELECT * FROM (SELECT *, row_number() over(partition by category_id, video_id order by ratio_likes_dislikes desc) as seqnum FROM global_temp.tags_popular) WHERE seqnum <= 10;")
    videos_ratio_best.show()
    videos_ratio_best.createGlobalTempView("videos_ratio_best")
    print("6 Query END ////////////////////////////////////////////////////////////////////\n\n\n")

