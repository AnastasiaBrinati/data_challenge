from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col, when

# Crea la SparkSession
spark = SparkSession.builder \
    .appName("Filtra e assegna giurisdizioni per coordinate") \
    .getOrCreate()

# File
percorso_parquet = "data/20250523_Hackaton_SIAE.parquet"
percorso_excel = "data/20250523_Clients_Historical.xlsx"
percorso_jurisdiction = "data/jurisdictions.csv"

# Colonne da leggere
colonne_da_selezionare = ["poi_type", "id", "lat", "lon"]

# Legge il parquet
df_parquet = spark.read.parquet(percorso_parquet).select(*colonne_da_selezionare)

# Legge Excel con pandas
df_excel = pd.read_excel(percorso_excel)

if "id" not in df_excel.columns:
    raise ValueError("La colonna 'id' non esiste nel file Excel")

id_da_escludere = df_excel["id"].dropna().astype(str).unique().tolist()

# Cast id a string in Spark
df_parquet = df_parquet.withColumn("id", col("id").cast("string"))

# Filtra id da escludere
df_filtrato = df_parquet.filter(~col("id").isin(id_da_escludere))

print(f"Numero righe dataset dopo filtro: {df_filtrato.count()}")

# Leggi jurisdiction.csv con Spark
df_jurisdiction = spark.read.csv(percorso_jurisdiction, header=True, inferSchema=True)

# Esempio colonne attese in jurisdiction.csv:
# jurisdiction, lat_min, lat_max, lon_min, lon_max

# Costruiamo la colonna "jurisdiction" con condizioni sulle coordinate
condizione = None

for row in df_jurisdiction.collect():
    juris = row['jurisdiction']
    min_lat = row['lat_min']
    max_lat = row['lat_max']
    min_lon = row['lon_min']
    max_lon = row['lon_max']

    cond_corrente = (
        (col("lat") >= min_lat) & (col("lat") <= max_lat) &
        (col("lon") >= min_lon) & (col("lon") <= max_lon)
    )

    if condizione is None:
        condizione = when(cond_corrente, juris)
    else:
        condizione = condizione.when(cond_corrente, juris)

condizione = condizione.otherwise("Unknown")

df_filtrato = df_filtrato.withColumn("jurisdiction", condizione)

giurisdizioni = [row['jurisdiction'] for row in df_jurisdiction.select("jurisdiction").distinct().collect()]
print("Giurisdizioni da suddividere:", giurisdizioni)

# Stampa alcuni dati della prima giurisdizione per verifica
if giurisdizioni:
    prima_giurisdizione = giurisdizioni[0]
    print(f"\n--- Esempio dati per la giurisdizione '{prima_giurisdizione}' ---")
    df_giurisdizione_1 = df_filtrato.filter(col("jurisdiction") == prima_giurisdizione)
    df_giurisdizione_1.show(10)

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
