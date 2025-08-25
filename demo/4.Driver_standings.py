# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

final_df = spark.read.parquet(f'{presentation_path}/race_results')\
                     .filter('race_year in(2019,2020)')

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,desc
point_sum_df = final_df.groupBy('race_year','driver_name','driver_nationality','team')\
        .agg(sum('points').alias('Total_points'), count(when(final_df['position'] == 1, True)).alias('Win'))\
        .orderBy(desc('Total_points'))

# display(point_sum_df)

# COMMAND ----------

from pyspark.sql.functions import rank,desc
from pyspark.sql.window import Window
rank_system = Window.partitionBy('race_year').orderBy(desc('Total_points'),desc('win'))

rank_df = point_sum_df.withColumn('rank',rank().over(rank_system))


# display(rank_df)

# COMMAND ----------

rank_df.write.mode('overwrite').parquet(f'{presentation_path}/driver_standings_2020_2019')

# COMMAND ----------

