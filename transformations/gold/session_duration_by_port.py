# from pyspark import pipelines as dp
# from pyspark.sql import functions as F

# @dp.materialized_view(name="session_duration_by_port")
# def session_duration_by_port():
#   return (
#     spark.read.table("traffic_log_enriched")
#     .groupBy("dst_port")
#     .agg(F.avg("session_duration").alias("average_session_duration"))
#   )

