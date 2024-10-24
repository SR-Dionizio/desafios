# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df = airbnb_hosts.join(
    airbnb_guests,
    F.concat(airbnb_hosts.gender, airbnb_hosts.nationality)
    == F.concat(airbnb_guests.gender, airbnb_guests.nationality),
)


result = df.select("host_id", "guest_id").distinct()

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
