# Import your libraries
import pyspark.sql.functions as F

# Start writing code
total_counts = google_file_store.select(
    (F.size(F.split(F.col("contents"), r"\bbull\b")) - 1).alias("bull"),
    (F.size(F.split(F.col("contents"), r"\bbear\b")) - 1).alias("bear"),
).agg(F.sum("bull").alias("bull"), F.sum("bear").alias("bear"))


# To validate your solution, convert your final pySpark df to a pandas df
result = total_counts.toPandas().melt(var_name="word", value_name="nentry")
result
