# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = spotify_worldwide_daily_song_ranking \
    .filter(F.col('position') == 1)

result = df.groupBy('trackname') \
    .agg(F.count('*').alias('times_top1')) \
    .orderBy(F.desc('times_top1'))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()