from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="num_sessions_by_cell")
def num_sessions_by_cell():
  return (
    spark.read.table("traffic_log_enriched")
    .groupBy("cell_id")
    .count().alias("num_sessions")
  )

