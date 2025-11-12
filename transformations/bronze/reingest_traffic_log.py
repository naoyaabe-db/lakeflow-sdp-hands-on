# # このファイルのコードはワークショップの途中、データ品質についての実験でコメントアウトします

# from pyspark import pipelines as dp
# from pyspark.sql import functions as F

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")

# @dp.append_flow(name="traffic_log_cut_session_duration", target="traffic_log_raw")
# def traffic_log_cut_session_duration():
#     return (
#       spark.readStream.table("traffic_log_invalid")
#       .withColumn(
#         "session_duration", 
#         F.when(F.col("session_duration") > 3600, 3600).otherwise(F.col("session_duration"))
#       )
#     )
