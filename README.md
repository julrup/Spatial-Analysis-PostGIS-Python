# Spatial Analysis with PostGIS & Python

Python scripts and PostGIS SQL queries for automated spatial data processing and relationship mapping.

> Code comments and logs are written in Polish.

> Komentarze w kodzie oraz nazwy zmiennych i komunikaty logowania są w języku polskim.

## About

This project automates the loading of various spatial datasets into a PostgreSQL/PostGIS database and runs a set of spatial analyses using PostGIS functions. The data covers urban infrastructure elements such as parcels, trees, buildings, roads, footpaths, and car parks — all referenced in the Polish national coordinate system EPSG:2180.

## Repository Structure

```
─ main_postgis_script.py     # Main script: data loading + spatial analysis
─ Roads.sql                  # Road geometry data (WKB-encoded polygons)
─ Footpaths.sql              # Footpath geometry data (WKB-encoded polygons)
─ drzewa.csv                 # Tree point data (X/Y coordinates)
─ drzewa_wkb.csv             # Trees converted to WKB (auto-generated)
─ parcele.csv                # Parcel polygon data (WKT format)
─ parcele_wkb.csv            # Parcels converted to WKB (auto-generated)
─ ewid_id.csv                # Cadastral identifiers
─ Car_parks.sqlite           # Car park spatial data (SQLite/GeoPackage)
─ budynki/                   # Building data (zipped shapefile)
─ Raport analiz przestrzennych.pdf  # Analysis report
```

## Requirements

- Python 3.8+
- PostgreSQL with PostGIS extension enabled
- Python packages: `geopandas`, `pandas`, `sqlalchemy`, `shapely`, `psycopg2`

## Setup

1. Clone the repository and install dependencies.

2. Open `main_postgis_script.py` and set your database credentials at the top of the file:

```python
USER = "your_user"
PASS = "your_password"
HOST = "localhost"
PORT = 5432
DB = "your_database"
```

3. Set the path to your local data files:

```python
BASE_DATA_PATH = r'path/to/your/data'
```

4. Make sure the `spatial` schema exists in your PostgreSQL database:

```sql
CREATE SCHEMA IF NOT EXISTS spatial;
CREATE EXTENSION IF NOT EXISTS postgis;
```

## Usage

Run the main script:

```bash
python main_postgis_script.py
```

The script will:

1. Convert CSV files (WKT/XY → WKB) for parcels and trees
2. Load all datasets into the `spatial` schema in PostGIS
3. Reproject data from EPSG:2178 to EPSG:2180 where needed
4. Create GIST spatial indexes on all geometry columns
5. Execute 15 spatial analysis queries and print the results

## Spatial Analyses

The script runs the following analyses using PostGIS:

| # | Analysis |
|---|----------|
| 1 | 5 nearest trees to a building |
| 2 | 10 largest parcels by area |
| 3 | Number of trees per parcel (top 4) |
| 4 | Buildings inside parcels larger than 100 m² |
| 5 | 5 nearest car parks to a given point |
| 6 | Number of car parks per parcel (top 2) |
| 7 | 7 nearest roads to a selected car park |
| 8 | Centroids of 3 buildings |
| 9 | Total length of all roads |
| 10 | Parcel with the highest number of trees |
| 11 | 5 buildings nearest to a selected tree |
| 12 | 3 nearest trees to each car park (within 200 m) |
| 13 | 5 longest roads |
| 14 | 10 buildings with the most car parks within 5 m |
| 15 | 3 parcels with the most buildings |

## Technologies

- **Python** — data processing and automation
- **GeoPandas** — spatial data I/O and format conversion
- **SQLAlchemy** — database connection and query execution
- **PostGIS** — spatial indexing and analysis (`ST_Distance`, `ST_Contains`, `ST_DWithin`, `ST_Area`, `ST_Length`, `ST_Centroid`, and more)
- **Shapely** — WKT/WKB geometry handling
- **Coordinate system** — EPSG:2180 (Poland CS92)
