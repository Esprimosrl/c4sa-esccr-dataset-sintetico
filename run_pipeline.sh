#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PYTHON="${PYTHON:-./bin/python}"
SEED="${SEED:-42}"
ANNO="${ANNO:-2024}"

"$PYTHON" dataset/data_gathering/anncsu_downloader.py \
  --out data_csv_parquet/civici_andria.csv

"$PYTHON" dataset/data_gathering/ispria_downloader.py \
  --anno "$ANNO" \
  --out data_csv_parquet/totali.csv

"$PYTHON" dataset/distribuzione_annuale.py \
  --civici data_csv_parquet/civici_andria.csv \
  --totali data_csv_parquet/totali.csv \
  --out data_csv_parquet/andria_distribuzione_annuale.csv \
  --seed "$SEED" \
  --totali-separator ','

"$PYTHON" dataset/query_merge_zone.py

"$PYTHON" dataset/query_inferisci_zone_mancanti.py

"$PYTHON" dataset/data_gathering/full_wms_zone_dump.py \
  --out data_csv_parquet/andria_full_zone.geojson

"$PYTHON" dataset/data_gathering/scrape_wms.py \
  --x-field "COORD_X_COMUNE" \
  --y-field "COORD_Y_COMUNE" \
  --sleep 5.3 \
  --polygons-out data_csv_parquet/poligoni_wms.geojson \
  data_csv_parquet/civici_andria.csv \
  --polygons-source data_csv_parquet/andria_full_zone.geojson \
  data_csv_parquet/civici_andria_classificati_per_zone_di_costruzione.csv

"$PYTHON" dataset/query_togli_civici_dalla_strada.py

"$PYTHON" dataset/query_crea_distribuzione_annuale_con_zone_di_costruzione.py \
  --civici-zone data_csv_parquet/civici_andria_riassegnati_no_viab_chunked_moved.csv

"$PYTHON" dataset/distribuzione_giornaliera.py \
  --annuo data_csv_parquet/andria_distribuzione_annuale_zone_inferite_e_zone_costruzione.csv \
  --anno "$ANNO" \
  --calendario data_csv_parquet/calendario_raccolta.csv \
  --id-col "id_civico" \
  --zona-col "zona_raccolta" \
  --seed "$SEED" \
  --out data_csv_parquet/conferimenti_giornalieri_classificati.csv \
  --out-produzione data_csv_parquet/produzione_giornaliera_classificata.csv \
  --missing-calendar-mode daily
