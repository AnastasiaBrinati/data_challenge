import pandas as pd

# Percorso del file Parquet da convertire
input_parquet = 'output/jurisdictions/J6/J6.parquet'

# Percorso del file CSV in uscita
output_csv = 'J6.csv'

# Carica il file Parquet
df = pd.read_parquet(input_parquet)

# Salva come CSV
df.to_csv(output_csv, index=False)

print(f"File convertito con successo: {output_csv}")
