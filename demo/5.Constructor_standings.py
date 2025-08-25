# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

final_df = spark.read.parquet(f'{presentation_path}/race_results')\
                    # .filter('race_year in (2019,2020)')

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,desc,rank
from pyspark.sql.window import Window
group_by_thing = final_df.groupBy('race_year','team')\
                         .agg(sum('points').alias('Total_points'),count(when(final_df['position'] == 1, True)).alias('Wins'))

partitioned = Window.partitionBy('race_year').orderBy(desc('Total_points'), desc('Wins'))

rank_df = group_by_thing.withColumn('rank', rank().over(partitioned))\
                        .filter('race_year == 2020')

# display(rank_df)


# COMMAND ----------

rank_df.write.parquet(f'{presentation_path}/constructor_standings_2020')

# COMMAND ----------

