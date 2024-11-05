# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = yelp_business.groupBy("business_id") \
    .agg(F.sum("review_count").alias("review_sum"),
         F.first("name").alias("name_business")) \
    .sort(F.desc("review_sum")) \
    .select("name_business", "review_sum")\
    .limit(5)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()