# from pyspark import pipelines as dp
# from pyspark.sql import functions as F

# @dp.materialized_view(name="bytes_by_subscriber")
# def bytes_by_subscriber():
#   return (
#     spark.read.table("traffic_log_enriched")
#     .withColumn("bytes", F.expr("bytes_received + bytes_sent"))
#     .groupBy("imsi")
#     .agg(F.sum("bytes").alias("sum_bytes"))
#   )