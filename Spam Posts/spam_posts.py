# Import your libraries
import pyspark.sql.functions as F

# Start writing code
spam_posts = facebook_posts.filter(F.col("post_keywords").contains("spam"))


merged_data = facebook_post_views.alias("views")\
                .join(facebook_posts.alias("posts"),F.col("views.post_id") == F.col("posts.post_id"))

total_post_views_per_day = merged_data\
                            .groupBy(F.to_date(F.col("posts.post_date"))\
                            .alias("post_date"))\
                            .agg(F.countDistinct(F.col("posts.post_id"))\
                            .alias("total_views")
)

spam_post_views_per_day = merged_data\
                            .filter(F.col("posts.post_keywords")\
                            .contains("spam"))\
                            .groupBy(F.to_date(F.col("posts.post_date"))\
                            .alias("post_date"))\
                            .agg(F.countDistinct(F.col("posts.post_id"))\
                            .alias("spam_views"))

result = total_post_views_per_day\
            .join(spam_post_views_per_day,  on="post_date", how="left")\
            .fillna(0)

result = result\
            .withColumn( "spam_percentage",(F.col("spam_views") / F.col("total_views")) * 100)\
            .select("post_date","spam_percentage")


# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
