from pyspark.sql import SparkSession
from pyspark_example_module import test_function
import time

appName = "BDP-PySpark Example"

# Create Spark session
spark = SparkSession.builder.appName(appName).getOrCreate()

# Call the function
test_function()

rdd = spark.sparkContext.parallelize([1,2,3,4])
print(rdd.collect())

time.sleep(60)