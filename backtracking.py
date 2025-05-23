import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import os
from glob import glob

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def calculate_travel_time(distance_meters):
    walking_speed_m_per_min = 5000 / 60
    return distance_meters / walking_speed_m_per_min

def trova_giurisdizione_per_punto(start_lat, start_lon, data_dir='./data/jurisdiction'):
    files = glob(os.path.join(data_dir, '*.parquet'))
    min_distance = float('inf')
    best_file = None
    nearest_poi = None

    for file in files:
        try:
            df_temp = pd.read_parquet(file)
        except Exception:
            continue
        if not {'lat_x', 'lon_x', 'jurisdiction'}.issubset(df_temp.columns):
            continue

        df_temp['distance'] = df_temp.apply(lambda r: haversine_distance(start_lat, start_lon, r['lat_x'], r['lon_x']), axis=1)
        closest = df_temp.loc[df_temp['distance'].idxmin()]
        if closest['distance'] < min_distance:
            min_distance = closest['distance']
            best_file = file
            nearest_poi = closest

    if best_file and nearest_poi is not None:
        return nearest_poi['jurisdiction'], pd.read_parquet(best_file)
    return None, None

def trova_percorso_ottimale_parquet(starting_points_csv='punti_partenza.csv', data_dir='./data/jurisdiction'):
    try:
        starting_points = pd.read_csv(starting_points_csv)
    except Exception as e:
        print(f"Errore nel caricamento di '{starting_points_csv}': {e}")
        return

    results = []

    for idx, row in starting_points.iterrows():
        start_lat, start_lon = row['lat'], row['lon']
        target_jurisdiction, df = trova_giurisdizione_per_punto(start_lat, start_lon, data_dir)

        if df is None or len(df) < 8:
            print(f"Nessuna giurisdizione valida trovata per il punto {idx + 1}")
            continue

        pois = df.to_dict('records')
        best_path = None
        best_fee = -1
        best_time = -1

        for start_candidate in pois:
            current_path = []
            current_fee = 0
            current_time = 0

            unvisited = pois.copy()
            selected_ids = set()

            for _ in range(8):  # MAX 8 punti da visitare
                best_next = None
                best_score = -1
                for candidate in unvisited:
                    if candidate['id'] in selected_ids:
                        continue
                    last_lat, last_lon = (start_lat, start_lon) if not current_path else (current_path[-1]['lat_x'], current_path[-1]['lon_x'])
                    dist = haversine_distance(last_lat, last_lon, candidate['lat_x'], candidate['lon_x'])
                    travel_time = calculate_travel_time(dist)
                    return_dist = haversine_distance(candidate['lat_x'], candidate['lon_x'], start_lat, start_lon)
                    return_time = calculate_travel_time(return_dist)
                    time_if_added = current_time + travel_time + 5

                    if time_if_added + return_time <= 180:
                        score = candidate['fee_value'] / (travel_time + 1e-6)
                        if score > best_score:
                            best_score = score
                            best_next = candidate
                            best_next_time = travel_time

                if best_next is None:
                    break

                current_time += best_next_time + 5
                current_fee += best_next['fee_value']
                current_path.append(best_next)
                selected_ids.add(best_next['id'])

            if len(current_path) >= 1:
                return_time = calculate_travel_time(haversine_distance(current_path[-1]['lat_x'], current_path[-1]['lon_x'], start_lat, start_lon))
                final_time = current_time + return_time

                if final_time <= 180 and current_fee > best_fee:
                    best_fee = current_fee
                    best_time = final_time
                    best_path = current_path

        if best_path:
            print(f"→ Percorso {idx + 1} nella giurisdizione '{target_jurisdiction}': tempo totale = {best_time:.1f} minuti, guadagno totale = {best_fee:.2f}")

            route_num = idx + 1
            jurisdiction_id = target_jurisdiction
            route_id = f"R_{jurisdiction_id}_{route_num}"

            start_poi = {
                'poi_id': 0,
                'lat': start_lat,
                'lon': start_lon,
                'estimated_fee_value': 0,
                'jurisdiction_id': jurisdiction_id
            }

            end_poi = start_poi.copy()  # anche ritorno ha poi_id = 0

            path_with_start_end = [start_poi] + [
                {
                    'poi_id': p['id'],
                    'lat': p['lat_x'],
                    'lon': p['lon_x'],
                    'estimated_fee_value': p['fee_value'],
                    'jurisdiction_id': jurisdiction_id
                } for p in best_path
            ] + [end_poi]

            for i, p in enumerate(path_with_start_end, start=1):
                p['stop_order'] = i
                p['route_id'] = route_id

            results.extend(path_with_start_end)

    if results:
        final_df = pd.DataFrame(results)
        final_df = final_df[[
            'route_id',
            'jurisdiction_id',
            'stop_order',
            'poi_id',
            'lat',
            'lon',
            'estimated_fee_value'
        ]]
        final_df.to_csv("percorsi_dettagliati.csv", index=False)
        print(f"\nTutti i percorsi dettagliati sono stati salvati in 'percorsi_dettagliati.csv'")
    else:
        print("Nessun percorso valido trovato.")

if __name__ == "__main__":
    trova_percorso_ottimale_parquet()
