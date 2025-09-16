from pyspark.sql import SparkSession
import sys
import os

# Ambil argumen dari SparkSubmitOperator
input_path = sys.argv[1]   # /mnt/data/input.csv
output_path = sys.argv[2]  # /mnt/data/output/

# Buat SparkSession
spark = SparkSession.builder \
    .appName("AirflowSparkTest") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

# Baca CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Contoh filter: hanya ambil baris yang amount > 0
if "amount" in df.columns:
    df_filtered = df.filter(df['amount'] > 0)
else:
    df_filtered = df

# Buat folder output jika belum ada
os.makedirs(output_path, exist_ok=True)
df_filtered.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
print(f"Spark job selesai, hasil di {output_path}")
