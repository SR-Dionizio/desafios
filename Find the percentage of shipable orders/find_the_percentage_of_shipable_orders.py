# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = orders.join(customers, "id")

df = 100 *(df.where(F.col("address").isNull()).count() / orders.count())

# To validate your solution, convert your final pySpark df to a pandas df
df