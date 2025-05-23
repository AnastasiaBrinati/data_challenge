from pyspark.sql import SparkSession

# Crea la SparkSession
spark = SparkSession.builder \
    .appName("Parquet to CSV") \
    .getOrCreate()

# Percorso del file Parquet di input
input_parquet_path = "prove\output\jurisdictions\J1\part-00000-249733fe-3cee-4e71-b2ee-fbb51e50deb1-c000.snappy.parquet"

# Percorso di output per il CSV
output_csv_path = "/output/final"

# Leggi il file Parquet
df = spark.read.parquet(input_parquet_path)

# Scrivi in formato CSV (includi intestazione e sovrascrivi se già esiste)
df.write.option("header", "true").mode("overwrite").csv(output_csv_path)

# Ferma la SparkSession
spark.stop()
