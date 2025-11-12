# from pyspark import pipelines as dp

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")

# # ストリーミングテーブルの作成
# dp.create_streaming_table("traffic_log_raw")

# # 東日本エリアの通信ログを取り込むAppendフロー
# @dp.append_flow(name="traffic_log_east", target="traffic_log_raw")
# def traffic_log_east():
#   return (
#     spark.readStream.format("cloudFiles")
#      .option("cloudFiles.format", "csv")
#      .option("cloudFiles.inferColumnTypes", "true")
#      .option("cloudFiles.rescuedDataColumn", "_rescued_data_traffic")
#      .load(f"/Volumes/{catalog_name}/{schema_name}/raw_data/network_traffic_log_east/")
#   )

# # 西日本エリアの通信ログを取り込むAppendフロー
# @dp.append_flow(name="traffic_log_west", target="traffic_log_raw")
# def traffic_log_west():
#   return (
#     spark.readStream.format("cloudFiles")
#      .option("cloudFiles.format", "csv")
#      .option("cloudFiles.inferColumnTypes", "true")
#      .option("cloudFiles.rescuedDataColumn", "_rescued_data_traffic")
#      .load(f"/Volumes/{catalog_name}/{schema_name}/raw_data/network_traffic_log_west/")
#   )