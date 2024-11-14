# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = olympics_athletes_events.groupBy("games").agg(
    F.countDistinct(F.col("name")).alias("athletes_number"))

result = df.filter(
    df['athletes_number'] == df.select(F.max('athletes_number')).first()[0])

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()