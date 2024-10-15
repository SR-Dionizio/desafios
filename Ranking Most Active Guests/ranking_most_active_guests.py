# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Start writing code
df = airbnb_contacts.groupby('id_guest')\
        .agg(F.sum('n_messages')\
        .alias('n_messages')).orderBy(F.desc('n_messages'), 'id_guest')
df = df.withColumn('ranking', F.dense_rank().over(Window.orderBy(F.desc('n_messages'))))

# To validate your solution, convert your final pySpark df to a pandas df
result = df.toPandas()
