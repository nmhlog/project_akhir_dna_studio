from pyspark.sql import SparkSession
import sys
import os
import shutil

# Ambil argumen dari SparkSubmitOperator
input_path = sys.argv[1]   # contoh: /mnt/data/input.csv
output_path = sys.argv[2]  # contoh: /mnt/data/output/

# Lokasi sementara di container (tidak dimount)
tmp_output = "/tmp/output_spark"

# Buat SparkSession
spark = SparkSession.builder \
    .appName("AirflowSparkTest") \
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

# Simpan hasil ke folder sementara (dalam container)
df_filtered.write.mode("overwrite").csv(f"file://{tmp_output}", header=True)

spark.stop()

# --------- Copy hasil ke folder tujuan (tanpa metadata chmod) ---------

# Hapus output lama kalau ada
if os.path.exists(output_path):
    for root, dirs, files in os.walk(output_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            shutil.rmtree(os.path.join(root, name))

# Buat folder tujuan
os.makedirs(output_path, exist_ok=True)

# Copy isi dari tmp_output ke output_path
for root, dirs, files in os.walk(tmp_output):
    rel_path = os.path.relpath(root, tmp_output)
    dest_dir = os.path.join(output_path, rel_path)
    os.makedirs(dest_dir, exist_ok=True)
    for file in files:
        shutil.copy(os.path.join(root, file), dest_dir)

print(f"✅ Spark job selesai, hasil ada di {output_path}")
