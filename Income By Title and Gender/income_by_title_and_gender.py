# Import your libraries
import pyspark.sql.functions as F


# Start writing code
bonus = sf_bonus.groupBy('worker_ref_id') \
                      .agg(F.sum('bonus').alias('bonus')) \
                      .select(F.col('worker_ref_id').alias('id'),'bonus')
result = bonus.join(sf_employee,on = 'id') \
                .groupBy('employee_title','sex') \
                .agg((F.avg('salary') + F.avg('bonus')).alias('avg_total_comp'))

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()
