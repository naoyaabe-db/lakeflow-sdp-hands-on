# from pyspark import pipelines as dp

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")

# # 加入者マスターのChange Data Feed (CDF)を読み込んで一時的なビューにする
# @dp.view(name="subscriber_cdf")
# def subscriber_cdf():
#     return (
#       spark.readStream.format("cloudFiles")
#       .option("cloudFiles.format", "csv")
#       .option("cloudFiles.inferColumnTypes", "true")
#       .option("cloudFiles.rescuedDataColumn", "_rescued_data_subscriber")
#       .load(f"/Volumes/{catalog_name}/{schema_name}/raw_data/subscriber_master_cdf/")
#     )

# # 加入者マスターを格納する空のストリーミングテーブルを作成
# dp.create_streaming_table(name="subscriber_master")

# # AutoCDCを使って、加入者のCDFに基づいて加入者マスターを差分更新する
# dp.create_auto_cdc_flow(
#   name="subscriber_master_auto_cdc",
#   target="subscriber_master",
#   source="subscriber_cdf",
#   keys=["imsi"],
#   sequence_by="subscriber_last_updated",
#   apply_as_deletes="operation='DELETE'",
#   stored_as_scd_type=2
# )

