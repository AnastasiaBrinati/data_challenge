from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col, when

# Crea la SparkSession
spark = SparkSession.builder \
    .appName("Filtra e assegna giurisdizioni per coordinate") \
    .getOrCreate()

# Percorso dei file
csv_file_path = "data/df_stime_medie.csv"
percorso_jurisdiction = "data/jurisdictions.csv"
percorso_dati_storici = "data/20250523_Clients_Historical.xlsx"

# Leggi il file Excel con Pandas per ottenere gli id da escludere
df_storici = pd.read_excel(percorso_dati_storici)
id_storici = df_storici['id'].tolist()

# Leggi il CSV principale in Spark
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Filtra il DataFrame Spark escludendo righe con id presenti in dati storici
df = df.filter(~col("id").isin(id_storici))

# Mostra le prime righe filtrate
print("DataFrame dopo filtro su id:")
df.show(5)

# Leggi jurisdiction.csv con Spark
df_jurisdiction = spark.read.csv(percorso_jurisdiction, header=True, inferSchema=True)

# Costruisci la colonna "jurisdiction" con condizioni sulle coordinate
condizione = None
for row in df_jurisdiction.collect():
    juris = row['jurisdiction']
    min_lat = row['lat_min']
    max_lat = row['lat_max']
    min_lon = row['lon_min']
    max_lon = row['lon_max']

    cond_corrente = (
        (col("lat_x") >= min_lat) & (col("lat_x") <= max_lat) &
        (col("lon_x") >= min_lon) & (col("lon_x") <= max_lon)
    )

    if condizione is None:
        condizione = when(cond_corrente, juris)
    else:
        condizione = condizione.when(cond_corrente, juris)

condizione = condizione.otherwise("Unknown")

df_filtrato = df.withColumn("jurisdiction", condizione)

# Lista delle giurisdizioni distinte
giurisdizioni = [row['jurisdiction'] for row in df_jurisdiction.select("jurisdiction").distinct().collect()]
print("Giurisdizioni da suddividere:", giurisdizioni)

# Mostra esempio dati per la prima giurisdizione
if giurisdizioni:
    prima_giurisdizione = giurisdizioni[0]
    print(f"\n--- Esempio dati per la giurisdizione '{prima_giurisdizione}' ---")
    df_filtrato.filter(col("jurisdiction") == prima_giurisdizione).show(10)

# Salvataggio dei dataset divisi per giurisdizione
output_base_path = "output/jurisdictions"

for juris in giurisdizioni:
    df_subset = df_filtrato.filter(col("jurisdiction") == juris)
    output_path = f"{output_base_path}/{juris}"
    df_subset.write.mode("overwrite").parquet(output_path)
    print(f"Salvato dataset per giurisdizione '{juris}' in {output_path}")

# Salva anche i dati non assegnati ("Unknown")
df_unknown = df_filtrato.filter(col("jurisdiction") == "Unknown")
output_unknown_path = f"{output_base_path}/Unknown"
df_unknown.write.mode("overwrite").parquet(output_unknown_path)
print(f"Salvato dataset per giurisdizione 'Unknown' in {output_unknown_path}")

spark.stop()
