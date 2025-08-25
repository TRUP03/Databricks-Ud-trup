# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

final_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct
count_df = final_df.filter("race_year == 2020").select(countDistinct('race_name'))

display(count_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum
driver_point_df = final_df.filter("driver_name == 'Lewis Hamilton'").select(sum('points'),countDistinct('race_name'))
display(driver_point_df)

# COMMAND ----------

group = final_df.groupBy('driver_name')\
                .agg(sum('points'),countDistinct('points')).orderBy('driver_name').show()

# COMMAND ----------

for_partition_df = spark.read.parquet(f'{presentation_path}/race_results')\
                             .filter("race_year in (2019,2020)")
# display(for_partition_df.groupBy('race_year').agg(count('*')))
# display(for_partition_df)

# COMMAND ----------

from pyspark.sql.functions import rank,desc
from pyspark.sql.window import Window
partitioned = Window.partitionBy('race_year').orderBy(desc('points'))

partitioned_df = for_partition_df.withColumn('rank', rank().over(partitioned))

display(partitioned_df)


# COMMAND ----------

