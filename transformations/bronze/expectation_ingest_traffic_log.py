# # このファイルのコードはワークショップの途中、データ品質についての実験でコメントアウトします

# from pyspark import pipelines as dp
# from pyspark.sql import functions as F
# from utilities import expectation_rules

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")

# # データ品質を担保するためのExpectationを定義
# raw_traffic_log_rules = expectation_rules.get_raw_traffic_log_rules() # 追加ポイント
# validation_filter = " AND ".join(raw_traffic_log_rules.values()) # 追加ポイント

# # 追加ポイント：振り分け前のログを格納するストリーミングテーブルの作成
# dp.create_streaming_table(
#     name="traffic_log_all",
#     expect_all=raw_traffic_log_rules,
# )

# # 東日本エリアの通信ログを取り込むAppendフロー
# @dp.append_flow(name="traffic_log_east", target="traffic_log_all") # 変更ポイント：targetをrawから
# def traffic_log_east():
#   return (
#     spark.readStream.format("cloudFiles")
#      .option("cloudFiles.format", "csv")
#      .option("cloudFiles.inferColumnTypes", "true")
#      .option("cloudFiles.rescuedDataColumn", "_rescued_data_traffic")
#      .load(f"/Volumes/{catalog_name}/{schema_name}/raw_data/network_traffic_log_east/")
#      .withColumn("valid", F.expr(validation_filter)) # 追加ポイント
#   )

# # 西日本エリアの通信ログを取り込むAppendフロー
# @dp.append_flow(name="traffic_log_west", target="traffic_log_all") # 変更ポイント：targetをrawから
# def traffic_log_west():
#   return (
#     spark.readStream.format("cloudFiles")
#      .option("cloudFiles.format", "csv")
#      .option("cloudFiles.inferColumnTypes", "true")
#      .option("cloudFiles.rescuedDataColumn", "_rescued_data_traffic")
#      .load(f"/Volumes/{catalog_name}/{schema_name}/raw_data/network_traffic_log_west/")
#      .withColumn("valid", F.expr(validation_filter)) # 追加ポイント
#   )

# # データ品質チェックをクリアしたデータを格納するストリーミングテーブル
# @dp.table(name="traffic_log_raw")
# def traffic_log_raw():
#   return (
#     spark.readStream.table("traffic_log_all")
#     .filter("valid = true")
#     .drop("valid")
#   )

# # データ品質に問題があったデータを格納するストリーミングテーブル
# @dp.table(name="traffic_log_invalid")
# def traffic_log_invalid():
#   return (
#     spark.readStream.table("traffic_log_all")
#     .filter("valid = false")
#     .drop("valid")
#   )