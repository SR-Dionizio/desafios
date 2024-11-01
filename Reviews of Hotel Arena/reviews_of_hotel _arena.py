import pyspark.sql.functions as F

# Start writing code
df = hotel_reviews\
    .filter(hotel_reviews['hotel_name'] == 'Hotel Arena')

result = df.groupby(['reviewer_score','hotel_name'])\
    .agg(F.count('*').alias('n_reviews'))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()