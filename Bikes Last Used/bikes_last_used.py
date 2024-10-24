# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = dc_bikeshare_q1_2012.groupBy("bike_number").agg(
    F.max("end_time").alias("last_time_used")
)

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
