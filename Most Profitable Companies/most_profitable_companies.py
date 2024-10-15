# Import your libraries
from pyspark.sql import functions as F

# Start writing code
forbes_global_2010_2014 = forbes_global_2010_2014.orderBy(
    F.col("profits").desc()).limit(3)

forbes_global_2010_2014 = forbes_global_2010_2014.select(
    "company",
    "profits"
)

# To validate your solution, convert your final pySpark df to a pandas df
forbes_global_2010_2014.toPandas()
