# from pyspark import pipelines as dp
# from pyspark.sql import functions as F

# @dp.materialized_view(name="throuput_by_location")
# def throughput_by_location():
#   return (
#     spark.read.table("traffic_log_enriched")
#     .groupBy("location")
#     .agg(F.avg("throughput_mbps").alias("average_throughput_mbps"))
#   )

