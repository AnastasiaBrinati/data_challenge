from pyspark.sql import SparkSession

# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Unione CSV") \
    .getOrCreate()

# Percorsi ai 6 file CSV
file_paths = [
    "J1.csv",
    "J2.csv",
    "J3.csv",
    "J4.csv",
    "J5.csv",
    "J6.csv"
]

# Leggi e unisci i CSV
df_unito = spark.read.option("header", True).csv(file_paths)

# Scrivi il risultato in un singolo file CSV
df_unito.coalesce(1).write.option("header", True).csv("finale.csv")

# Ferma la sessione Spark
spark.stop()
