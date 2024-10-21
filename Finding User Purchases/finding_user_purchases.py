# Import your libraries
import pyspark.sql.functions as F
from pyspark.sql.window import Window


# Start writing code
df = amazon_transactions.orderBy(F.col("user_id"), F.col("created_at"))

windowSpec = Window.partitionBy("user_id").orderBy("created_at")


df = df.withColumn("previous_transaction_date", F.lag("created_at").over(windowSpec))


df = df.withColumn(
    "days_diff", F.datediff(F.col("created_at"), F.col("previous_transaction_date"))
)

df = df.filter(
    F.col("previous_transaction_date").isNotNull() & (F.col("days_diff") <= 7)
)


# To validate your solution, convert your final pySpark df to a pandas df
result = df.select("user_id").distinct().toPandas()
