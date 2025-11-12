# from pyspark import pipelines as dp

# # パラメータとして設定したカタログ名とスキーマ名を取得
# catalog_name = spark.conf.get("catalog_name")
# schema_name = spark.conf.get("schema_name")

# # 外部にある基地局マスターテーブルからデータを取り込むマテリアライズドビュー
# @dp.materialized_view(name="cell_master")
# def cell_master():
#   return spark.read.table(f"{catalog_name}.{schema_name}.cell_master_source")