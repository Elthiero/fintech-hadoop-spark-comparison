import findspark
import os
import sys
import time

# Environment Setup
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["JAVA_HOME"] = r"C:\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["HADOOP_CONF_DIR"] = r"C:\hadoop\etc\hadoop"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, when, col, round

spark = SparkSession.builder.appName("CardNetworkStatsRaw").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Point directly to the RAW dataset in HDFS
input_path = "hdfs://localhost:9000/fintech_project/train_transaction.csv"
output_path = "hdfs://localhost:9000/fintech_project/output_spark"

# Start time
start_time = time.time()

# Load the raw dataset
# Spark's built-in CSV parser will automatically handle the complex quotes and commas!
print("Loading CSV...")
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Replicate the MapReduce missing-data logic: fill nulls in 'card4' with 'unknown'
df = df.fillna({'card4': 'unknown'})

# Process using the raw column names
print("Processing ...")
result = df.groupBy("card4").agg(
    sum("TransactionAmt").alias("Total Amount"),
    avg("TransactionAmt").alias("Average Amount"),
    round((count(when(col("isFraud") == 1, True)) / count("*") * 100), 2).alias("Fraud (%)")
)

# Output
print("\nResult:")
result.show()

# Coalesce to 1 to prevent tiny fragmented part files in HDFS
result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# End time
end_time = time.time()
print(f"TOTAL EXECUTION TIME: {end_time - start_time:.2f} seconds") 

# Stop Spark session
spark.stop()