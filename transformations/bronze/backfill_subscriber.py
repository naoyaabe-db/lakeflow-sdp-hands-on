# # このファイルのコードはワークショップの途中、データ品質についての実験でコメントアウトします

# from pyspark import pipelines as dp

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")

# # バックフィルデータを格納したパスをパラメータから取得
# backfill_dir = spark.conf.get("backfill_dir")

# def setup_subscriber_backfill_flow(backfill_dir):
#     # 加入者マスターのChange Data Feed (CDF)を読み込んで一時的なビューにする
#     @dp.view(name=f"subscriber_backfill_{backfill_dir}")
#     def subscriber_cdf_backfill():
#         return (
#           spark.readStream.format("cloudFiles")
#           .option("cloudFiles.format", "csv")
#           .option("cloudFiles.inferColumnTypes", "true")
#           .option("cloudFiles.rescuedDataColumn", "_rescued_data_subscriber")
#           .load(f"/Volumes/{catalog_name}/{schema_name}/raw_data/{backfill_dir}/")
#         )
#     # AutoCDCを使って、加入者のCDFに基づいて加入者マスターを差分更新する
#     dp.create_auto_cdc_flow(
#       name=f"subscriber_auto_cdc_backfill_{backfill_dir}",
#       target="subscriber_master",
#       source=f"subscriber_backfill_{backfill_dir}",
#       keys=["imsi"],
#       sequence_by="subscriber_last_updated",
#       apply_as_deletes="operation='DELETE'",
#       stored_as_scd_type=2,
#       # 一回だけ実行する
#       once=True
#     )

# # パラメータに設定されているバックフィルデータのディレクトリの分だけ、フローを作成する
# setup_subscriber_backfill_flow(backfill_dir)