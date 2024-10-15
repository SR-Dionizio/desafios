# Import your libraries
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window


# Start writing code
salary_per_dept = (
    db_employee
    .join(db_dept.alias("d"), db_employee['department_id'] == F.col("d.id"))\
    .groupBy('d.department')\
    .agg(F.max('salary').alias('max_salary'))
)

eng_salary = salary_per_dept.filter(F.col('department') == 'engineering')\
                .select(F.col('max_salary').alias('eng_max'))

mar_salary = salary_per_dept.filter(F.col('department') == 'marketing')\
                .select(F.col('max_salary').alias('mar_max'))

salary_diff = eng_salary.crossJoin(mar_salary).select(
    F.abs(F.col('eng_max') - F.col('mar_max')).alias('salary_difference')
)

# To validate your solution, convert your final pySpark df to a pandas df
salary_diff.toPandas()
