"""Shared configuration for the dataset pipeline."""

DEFAULT_ANNO = 2024
DEFAULT_SEED = 42
DEFAULT_JOBS = 2
DEFAULT_TOTALS_SEPARATOR = ","
DEFAULT_WMS_SLEEP_SECONDS = 5.3
DEFAULT_MISSING_CALENDAR_MODE = "daily"
DEFAULT_ANNUAL_GRID_METERS = 120.0
DEFAULT_DAILY_SPILLOVER_DAYS = 14
DEFAULT_DAILY_ZONE = "1"
DEFAULT_WMS_CRS = "CRS:84"
DEFAULT_WMS_DELTA_DEGREES = 0.0005
DEFAULT_WMS_DELTA_METERS = 5.0
DEFAULT_WMS_QUERY_SLEEP_SECONDS = 0.05
DEFAULT_REASSIGN_CELL_DEGREES = 0.000001
DEFAULT_REASSIGN_ZONES_PER_CHUNK = 3

ANNCSU_ZIP_URL = "https://anncsu.open.agenziaentrate.gov.it/age-inspire/opendata/anncsu/getds.php?INDIR_PUGL"
ANDRIA_ISTAT_CODE = "110001"
ANNCSU_WORK_DIR = "anncsu_stradario_puglia_andria"
ISPRA_WORK_DIR = "ispra_andria_2024"
WMS_BASE_URL = "https://comune-andria.planetek.it/ows/3/qdjango/6/"
WMS_LAYER = "prg_andria_v4_7b29ddc5_9192_4576_b367_430a4d102c81"

CIVICI_CSV = "data_csv_parquet/civici_andria.csv"
TOTALI_CSV = "data_csv_parquet/totali.csv"
ANNUAL_CSV = "data_csv_parquet/andria_distribuzione_annuale.csv"
STRADARIO_ZONE_CSV = "data_csv_parquet/andria_stradario_zone.csv"
ANNUAL_ZONE_CSV = "data_csv_parquet/andria_distribuzione_annuale_zone.csv"
ANNUAL_ZONE_INFERRED_CSV = "data_csv_parquet/andria_distribuzione_annuale_zone_inferite.csv"
FULL_WMS_ZONE_GEOJSON = "data_csv_parquet/andria_full_zone.geojson"
WMS_POLYGONS_GEOJSON = "data_csv_parquet/poligoni_wms.geojson"
CIVICI_PRG_CSV = "data_csv_parquet/civici_andria_classificati_per_zone_di_costruzione.csv"
CIVICI_PRG_REASSIGNED_CSV = "data_csv_parquet/civici_andria_riassegnati_no_viab_chunked_moved.csv"
ANNUAL_COMPLETE_PARQUET = "data_csv_parquet/andria_distribuzione_annuale_completa.parquet"
CALENDARIO_RACCOLTA_CSV = "data_csv_parquet/calendario_raccolta.csv"
CONFERIMENTI_GIORNALIERI_PARQUET = "data_csv_parquet/conferimenti_giornalieri_completi.parquet"
PRODUZIONE_GIORNALIERA_PARQUET = "data_csv_parquet/produzione_giornaliera_completa.parquet"

ANNCSU_DOWNLOADER = "dataset/data_gathering/anncsu_downloader.py"
ISPRA_DOWNLOADER = "dataset/data_gathering/ispria_downloader.py"
ANNUAL_DISTRIBUTION = "dataset/distribuzione_annuale.py"
MERGE_ZONE = "dataset/query_merge_zone.py"
INFER_ZONE = "dataset/query_inferisci_zone_mancanti.py"
FULL_WMS_ZONE_DUMP = "dataset/data_gathering/full_wms_zone_dump.py"
WMS_SCRAPER = "dataset/data_gathering/scrape_wms.py"
REASSIGN_CIVICI = "dataset/query_togli_civici_dalla_strada.py"
ADD_BUILDING_ZONE = "dataset/query_crea_distribuzione_annuale_con_zone_di_costruzione.py"
DAILY_DISTRIBUTION = "dataset/distribuzione_giornaliera.py"

WMS_X_FIELD = "COORD_X_COMUNE"
WMS_Y_FIELD = "COORD_Y_COMUNE"
DAILY_ID_COLUMN = "id_civico"
DAILY_ZONE_COLUMN = "zona_raccolta"
