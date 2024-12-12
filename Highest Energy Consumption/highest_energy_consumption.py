# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window


# Start writing code

window_spec = Window.orderBy(F.desc('consumption'))

result = (
    fb_eu_energy
        .unionAll(fb_asia_energy)
        .unionAll(fb_na_energy)
        .groupBy('date')
        .agg(F.sum('consumption').alias('consumption'))
        .withColumn('rank', F.rank().over(window_spec))
        .where(F.col('rank') == 1)
        .drop('rank')
)

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()