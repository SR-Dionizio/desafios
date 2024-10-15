# Import your libraries
import pyspark.sql.functions as F


# Start writing code
ms_download_facts = ms_download_facts.join(
    ms_user_dimension, ms_download_facts.user_id == ms_user_dimension.user_id, "left"
).join(
    ms_acc_dimension, ms_user_dimension.acc_id == ms_acc_dimension.acc_id
)

ms_download_facts = ms_download_facts.groupBy("date", "paying_customer").agg(
    F.sum("downloads").alias("total_downloads")
)

ms_download_facts = ms_download_facts.groupBy("date").pivot("paying_customer").sum(
    "total_downloads"
)

ms_download_facts = ms_download_facts.withColumnRenamed(
    "no", "non_paying_downloads"
).withColumnRenamed(
    "yes", "paying_downloads"
)

ms_download_facts = ms_download_facts.filter(
    F.col("non_paying_downloads") > F.col("paying_downloads")
)

ms_download_facts = ms_download_facts.orderBy(
    F.col("date").asc()
)

ms_download_facts = ms_download_facts.select(
    "date", "non_paying_downloads", "paying_downloads"
)

# To validate your solution, convert your final pySpark df to a pandas df
ms_download_facts.toPandas()

