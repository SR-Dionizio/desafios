import pyspark.sql.functions as F

merged = playbook_events.join(playbook_users, on="user_id")
apple_device = ["macbook pro", "iphone 5s", "ipad air"]
df = (
    merged.filter(F.col("device").isin(apple_device))
    .groupBy("language")
    .agg(F.countDistinct("user_id").alias("apple_users"))
)

df = (
    merged.groupBy("language")
    .agg(F.countDistinct("user_id").alias("total_users"))
    .orderBy(F.desc("total_users"))
    .join(df, on="language", how="left")
    .fillna(0)
    .select("language", "apple_users", "total_users")
)

df.toPandas()