# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = airbnb_units.join(airbnb_hosts, on='host_id')\
                 .filter((airbnb_hosts.age < 30) & (airbnb_units.unit_type == "Apartment"))\
                 .groupBy(airbnb_hosts["nationality"])\
                 .agg(F.countDistinct("unit_id").alias("apartment_count"))\
                 .sort(F.desc("apartment_count"))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()
