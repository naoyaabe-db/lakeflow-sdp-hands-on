# from pyspark import pipelines as dp
# from pyspark.sql import functions as F
# from utilities import expectation_rules

# joined_traffic_log_rules = expectation_rules.get_joined_traffic_log_rules()

# # データ品質チェック前のトラフィックログを入れた一時ビューにExpectationを適用
# @dp.expect_all_or_fail(joined_traffic_log_rules)
# @dp.table(name="traffic_log_enriched")
# def traffic_log_enriched():
#     return (
#         spark.readStream.table("traffic_log_raw")
#         .join(spark.read.table("cell_master"), "cell_id", "left")
#         .join(spark.read.table("subscriber_master"), "imsi", "left")
#     )