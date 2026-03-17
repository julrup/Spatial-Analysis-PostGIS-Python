import sqlalchemy
from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd
import os
import logging
from shapely import wkb
import binascii
import shapely.wkt
import shapely.wkb
import shapely.geometry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#add USER, PASS, HOST, PORT and DB here

BASE_DATA_PATH = r'C:\Use\Your\path'

DATA_FILES = {
    'parcele': 'parcele.csv',
    'drzewa': 'drzewa.csv',
    'roads': 'Roads.sql',
    'footpaths': 'Footpaths.sql',
    'budynki': 'budynki.zip',
    'parkingi': 'Car_parks.sqlite'
}

def create_db_engine(user, password, host, port, db):
    db_connection_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(db_connection_url)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("SELECT 1"))
    logger.info("Połączenie z bazą danych ustanowione pomyślnie")
    return engine

def load_sql_with_wkb_to_gdf(file_path, columns, crs):
    data_list = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if 'INSERT INTO' in line.upper():
                if 'VALUES' in line.upper():
                    values_part = line.split('VALUES')[1].strip()
                    values_part = values_part.strip('());')
                    if values_part.startswith('(') and values_part.endswith(')'):
                        values_part = values_part[1:-1]
                    values = []
                    current_value = ""
                    in_quotes = False
                    for char in values_part:
                        if char == "'" and (not current_value or current_value[-1] != '\\'):
                            in_quotes = not in_quotes
                        elif char == ',' and not in_quotes:
                            values.append(current_value.strip().strip("'"))
                            current_value = ""
                            continue
                        current_value += char
                    if current_value:
                        values.append(current_value.strip().strip("'"))
                    if values:
                        data_list.append(values)
    records = []
    geometries = []
    for row in data_list:
        if len(row) >= len(columns):
            record = {}
            for i, col in enumerate(columns):
                record[col] = row[i] if i < len(row) else None
            try:
                wkb_hex = record['wkb_geometry'].strip().strip("'\"")
                wkb_bytes = binascii.unhexlify(wkb_hex)
                geometry = wkb.loads(wkb_bytes)
                geometries.append(geometry)
            except Exception as e:
                logger.warning(f"Błąd konwersji geometrii WKB: {e}")
                geometries.append(None)
            records.append(record)
    gdf = gpd.GeoDataFrame(records, geometry=geometries, crs=crs)
    return gdf

def table_loader(gdf, name, engine):
    try:
        gdf.to_postgis(name, con=engine, if_exists='replace', index=False, schema="spatial")
        logger.info(f"Tabela '{name}' załadowana pomyślnie do bazy danych.")
        return True
    except Exception as e:
        logger.error(f"Błąd podczas ładowania tabeli '{name}': {e}")
        return False

def check_table_columns(engine, table_name):
    query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'spatial' AND table_name = '{table_name}' 
    ORDER BY ordinal_position;
    """
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(query)).fetchall()
        logger.info(f"Kolumny w tabeli '{table_name}':")
        for row in result:
            logger.info(f"  - {row[0]} ({row[1]})")
        return [row[0] for row in result]

def find_id_column(engine, table_name):
    columns = check_table_columns(engine, table_name)
    preferred_id_cols = ['objectid', 'id', 'fid', 'gid', 'ogc_fid']
    for preferred in preferred_id_cols:
        for col in columns:
            if col.lower() == preferred.lower():
                logger.info(f"Znaleziono kolumnę ID '{col}' w tabeli '{table_name}'")
                return col
    id_columns = [col for col in columns if 'id' in col.lower()]
    if id_columns:
        logger.info(f"Używam kolumny '{id_columns[0]}' jako ID w tabeli '{table_name}'")
        return id_columns[0]
    if columns:
        logger.info(f"Używam pierwszej kolumny '{columns[0]}' jako ID w tabeli '{table_name}'")
        return columns[0]
    logger.warning(f"Nie znaleziono odpowiedniej kolumny ID w tabeli '{table_name}'")
    return None

def run_query(sql_query, engine, fetch_results=False):
    with engine.connect() as connection:
        if fetch_results:
            result = connection.execute(sqlalchemy.text(sql_query)).fetchall()
            logger.info("Zapytanie wykonane pomyślnie - pobrano wyniki")
            if not result:
                print("Brak wyników dla tego zapytania.")
            else:
                for row in result:
                    formatted_row = []
                    for item in row:
                        if isinstance(item, str) and len(item) > 200:
                            formatted_row.append(item[:200] + "...")
                        else:
                            formatted_row.append(item)
                    print(tuple(formatted_row))
            return True
        else:
            connection.execute(sqlalchemy.text(sql_query))
            logger.info("Zapytanie wykonane pomyślnie")
            return True

def convert_parcele_csv_to_wkb(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    
    required_cols = ['wkt', 'idEwid']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Brakujące kolumny w pliku {input_csv}: {missing_cols}")
    
    df = df.dropna(subset=['wkt'])
    df['geometry'] = df['wkt'].apply(shapely.wkt.loads)
    df['wkb'] = df['geometry'].apply(lambda geom: binascii.hexlify(shapely.wkb.dumps(geom)).decode())
    df[['idEwid', 'wkb']].to_csv(output_csv, index=False)
    logger.info(f"Zapisano {output_csv} z kolumną WKB.")

def convert_drzewa_csv_to_wkb(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    
    required_cols = ['X', 'Y', 'OBJECTID']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Brakujące kolumny w pliku {input_csv}: {missing_cols}")
    
    df = df.dropna(subset=['X', 'Y'])
    df['geometry'] = df.apply(lambda row: shapely.geometry.Point(row['X'], row['Y']), axis=1)
    df['wkb'] = df['geometry'].apply(lambda geom: binascii.hexlify(shapely.wkb.dumps(geom)).decode())
    df[['OBJECTID', 'wkb']].to_csv(output_csv, index=False)
    logger.info(f"Zapisano {output_csv} z kolumną WKB.")

def load_csv_wkb_to_gdf(file_path, id_col, wkb_col, crs):
    df = pd.read_csv(file_path)
    df['geometry'] = df[wkb_col].apply(lambda wkb_hex: shapely.wkb.loads(binascii.unhexlify(wkb_hex)))
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=crs)
    return gdf

def polygons_to_lines(gdf):
    gdf['geometry'] = gdf['geometry'].boundary
    return gdf

def load_data_to_postgis():
    engine = create_db_engine(USER, PASS, HOST, PORT, DB)
    base_path = BASE_DATA_PATH
    logger.info("Rozpoczynam ładowanie danych do bazy PostGIS")
    logger.info(f"Używam ścieżki bazowej: {base_path}")

    parcele_csv = os.path.join(base_path, DATA_FILES['parcele'])
    parcele_wkb_csv = os.path.join(base_path, 'parcele_wkb.csv')
    convert_parcele_csv_to_wkb(parcele_csv, parcele_wkb_csv)

    drzewa_csv = os.path.join(base_path, DATA_FILES['drzewa'])
    drzewa_wkb_csv = os.path.join(base_path, 'drzewa_wkb.csv')
    convert_drzewa_csv_to_wkb(drzewa_csv, drzewa_wkb_csv)

    logger.info("Ładowanie parcele_wkb.csv przez WKB...")
    gdf_parcele = load_csv_wkb_to_gdf(parcele_wkb_csv, 'idEwid', 'wkb', crs="EPSG:2180")
    table_loader(gdf_parcele, "parcele", engine)

    logger.info("Ładowanie drzewa_wkb.csv przez WKB...")
    gdf_drzewa = load_csv_wkb_to_gdf(drzewa_wkb_csv, 'OBJECTID', 'wkb', crs="EPSG:2180")
    table_loader(gdf_drzewa, "drzewa", engine)

    logger.info("Ładowanie Roads.sql...")
    file_path_roads = os.path.join(base_path, DATA_FILES['roads'])
    columns_roads = ["wkb_geometry", "objectid", "fid_road", "absorptive",
                     "area", "surftype", "local_", "active", "shape_leng", "shape_area"]
    gdf_roads = load_sql_with_wkb_to_gdf(file_path_roads, columns_roads, crs="EPSG:2178")
    gdf_roads = gdf_roads.to_crs("EPSG:2180")
    gdf_roads = polygons_to_lines(gdf_roads)
    table_loader(gdf_roads, "roads", engine)

    logger.info("Ładowanie Footpaths.sql...")
    file_path_footpaths = os.path.join(base_path, DATA_FILES['footpaths'])
    columns_footpaths = ["wkb_geometry", "objectid", "fid_footpa", "local_",
                         "active", "surftype", "area", "shape_leng", "shape_area"]
    gdf_footpaths = load_sql_with_wkb_to_gdf(file_path_footpaths, columns_footpaths, crs="EPSG:2178")
    gdf_footpaths = gdf_footpaths.to_crs("EPSG:2180")
    gdf_footpaths = polygons_to_lines(gdf_footpaths)
    table_loader(gdf_footpaths, "footpaths", engine)

    logger.info("Ładowanie budynki.zip...")
    file_path_buildings = os.path.join(base_path, DATA_FILES['budynki'])
    gdf_buildings = gpd.read_file(file_path_buildings)
    if gdf_buildings.crs is None:
        logger.warning("Brak CRS w pliku budynków, ustawiam EPSG:2180")
        gdf_buildings.set_crs("EPSG:2180", inplace=True)
    elif gdf_buildings.crs != "EPSG:2180":
        gdf_buildings = gdf_buildings.to_crs("EPSG:2180")
    table_loader(gdf_buildings, "budynki", engine)

    logger.info("Ładowanie Car_parks.sqlite...")
    file_path_parkings = os.path.join(base_path, DATA_FILES['parkingi'])
    gdf_parkings = gpd.read_file(file_path_parkings)
    
    if gdf_parkings.crs is None:
        logger.warning("Brak CRS w pliku parkingów, ustawiam EPSG:2180")
        gdf_parkings.set_crs("EPSG:2180", inplace=True)
    elif gdf_parkings.crs != "EPSG:2180":
        gdf_parkings = gdf_parkings.to_crs("EPSG:2180")
    
    table_loader(gdf_parkings, "parkingi", engine)

    logger.info(f"Ładowanie danych zakończone")
    return engine

def create_spatial_indexes(engine):
    index_queries = [
        "CREATE INDEX IF NOT EXISTS trees_geom_idx ON spatial.drzewa USING GIST (geometry);",
        "CREATE INDEX IF NOT EXISTS buildings_geom_idx ON spatial.budynki USING GIST (geometry);",
        "CREATE INDEX IF NOT EXISTS parkingi_geom_idx ON spatial.parkingi USING GIST (geometry);",
        "CREATE INDEX IF NOT EXISTS footpaths_geom_idx ON spatial.footpaths USING GIST (geometry);",
        "CREATE INDEX IF NOT EXISTS roads_geom_idx ON spatial.roads USING GIST (geometry);"
    ]
    with engine.connect() as conn:
        for q in index_queries:
            try:
                conn.execute(sqlalchemy.text(q))
            except Exception:
                pass
    logger.info("Indeksy przestrzenne utworzone.")

def run_spatial_analysis(engine):
    logger.info("Rozpoczynam wykonywanie zadań analiz przestrzennych")
    budynki_id_col = find_id_column(engine, 'budynki')
    parkingi_id_col = find_id_column(engine, 'parkingi')
    queries = [
        {"name": "Zadanie 1: 5 najbliższych drzew od budynku", 
         "query": "SELECT d.\"OBJECTID\", ST_Distance(d.geometry, b.geometry) AS dist FROM spatial.drzewa d, (SELECT geometry FROM spatial.budynki LIMIT 1) b ORDER BY d.geometry <-> b.geometry LIMIT 5;"},
       
        {"name": "Zadanie 2: 10 największych parceli",
         "query": "SELECT \"idEwid\", ST_Area(geometry) AS area FROM spatial.parcele ORDER BY area DESC LIMIT 10;"},
       
        {"name": "Zadanie 3: Liczba drzew w każdej parceli (top 4)",
         "query": "SELECT p.\"idEwid\", COUNT(d.\"OBJECTID\") AS tree_count FROM spatial.parcele p LEFT JOIN spatial.drzewa d ON ST_Contains(p.geometry, d.geometry) GROUP BY p.\"idEwid\" ORDER BY tree_count DESC LIMIT 4;"},
       
        {"name": "Zadanie 4: Budynki w parcelach > 100 m²",
         "query": f"SELECT b.* FROM spatial.budynki b JOIN spatial.parcele p ON ST_Contains(p.geometry, b.geometry) WHERE ST_Area(p.geometry) > 100 LIMIT 5;"},
       
        {"name": "Zadanie 5: 5 najbliższych parkingów od punktu",
         "query": "SELECT *, ST_Distance(geometry, ST_SetSRID(ST_MakePoint(564800, 244900), 2180)) AS dist FROM spatial.parkingi ORDER BY geometry <-> ST_SetSRID(ST_MakePoint(564800, 244900), 2180) LIMIT 5;"},
        
        {"name": "Zadanie 6: Liczba parkingów w parceli (top 2)",
         "query": "SELECT p.\"idEwid\", COUNT(pk.*) AS parking_count FROM spatial.parcele p LEFT JOIN spatial.parkingi pk ON ST_Contains(p.geometry, pk.geometry) GROUP BY p.\"idEwid\" ORDER BY parking_count DESC LIMIT 2;"},
       
        {"name": "Zadanie 7: 7 najbliższych dróg do wybranego parkingu",
         "query": "SELECT r.objectid, ST_Distance(r.geometry, pk.geometry) AS dist FROM spatial.roads r, (SELECT geometry FROM spatial.parkingi LIMIT 1) pk ORDER BY dist ASC LIMIT 7;"},
       
        {"name": "Zadanie 8: Centroidy 3 budynków",
         "query": "SELECT *, ST_Centroid(geometry) AS centroid FROM spatial.budynki LIMIT 3;"},
    
        {"name": "Zadanie 9: Całkowita długość dróg",
         "query": "SELECT SUM(ST_Length(geometry)) AS total_length FROM spatial.roads;"},
    
        {"name": "Zadanie 10: Parcela o największej liczbie drzew",
         "query": "SELECT p.\"idEwid\", COUNT(d.\"OBJECTID\") AS tree_count FROM spatial.parcele p LEFT JOIN spatial.drzewa d ON ST_Contains(p.geometry, d.geometry) GROUP BY p.\"idEwid\" ORDER BY tree_count DESC LIMIT 1;"},
    
        {"name": "Zadanie 11: 5 budynków najbliższych do wybranego drzewa",
         "query": "SELECT b.\"OBJECTID\", ST_Distance(b.geometry, d.geometry) AS dist FROM spatial.budynki b, (SELECT geometry FROM spatial.drzewa LIMIT 1) d ORDER BY dist ASC LIMIT 5;"},
      
        {"name": "Zadanie 12: 3 najbliższe drzewa do każdego parkingu",
         "query": f"SELECT pk.\"{parkingi_id_col}\", d.\"OBJECTID\", ST_Distance(pk.geometry, d.geometry) AS dist FROM spatial.parkingi pk JOIN spatial.drzewa d ON ST_DWithin(pk.geometry, d.geometry, 200) ORDER BY pk.\"{parkingi_id_col}\", dist LIMIT 3;"},
      
        {"name": "Zadanie 13: 5 najdłuższych dróg",
         "query": "SELECT objectid, ST_Length(geometry) AS length FROM spatial.roads ORDER BY length DESC LIMIT 5;"},
      
        {"name": "Zadanie 14: 10 budynków o największej liczbie parkingów w promieniu 5 m",
         "query": "SELECT b.\"OBJECTID\", COUNT(pk.*) AS parking_count FROM spatial.budynki b LEFT JOIN spatial.parkingi pk ON ST_DWithin(b.geometry, pk.geometry, 5) GROUP BY b.\"OBJECTID\" ORDER BY parking_count DESC LIMIT 10;"},
      
        {"name": "Zadanie 15: 3 parcele o największej liczbie budynków",
         "query": "SELECT p.\"idEwid\", COUNT(b.\"OBJECTID\") AS building_count FROM spatial.parcele p LEFT JOIN spatial.budynki b ON ST_Contains(p.geometry, b.geometry) GROUP BY p.\"idEwid\" ORDER BY building_count DESC LIMIT 3;"}
    ]
    for q in queries:
        print(f"\n {q['name']}")
        run_query(q['query'], engine, fetch_results=True)
        print(f"Koniec wyników dla: {q['name']}\n")

def main():
    try:
        engine = load_data_to_postgis()
        create_spatial_indexes(engine)
        run_spatial_analysis(engine)
    except Exception as e:
        logger.error(f"Błąd krytyczny: {e}")
    finally:
        logger.info("Program zakończony")

if __name__ == "__main__":
    main()