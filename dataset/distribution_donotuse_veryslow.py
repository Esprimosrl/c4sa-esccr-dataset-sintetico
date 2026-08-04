import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors


# =========================
# CONFIG
# =========================

INPUT_CSV = "INDIR_PUGL_20260602.csv"
OUTPUT_CSV = "andria_synthetic_garbage_distribution.csv"

# Fixed CODICE_ISTAT for the municipality.
# Replace this with the actual ISTAT code used in your address file.
FIXED_CODICE_ISTAT = "110001"  # Andria example; verify your file value

# Coordinate columns in your address CSV
X_COL = "COORD_X_COMUNE"
Y_COL = "COORD_Y_COMUNE"

# Radius in coordinate units.
# If your coordinates are meters, 150 means 150 meters.
# If your coordinates are lon/lat degrees, reproject before using this method.
DENSITY_RADIUS = 150

# Controls how strongly dense clusters produce more waste.
# 1.0 = linear with density
# >1.0 = dense areas get much more weight
# <1.0 = smoother, less concentrated
DENSITY_EXPONENT = 1.15

# Small randomness so points with similar density are not identical.
# Set to 0 for deterministic purely density-based output.
RANDOM_NOISE = 0.15

RANDOM_SEED = 42


# =========================
# WASTE TOTALS
# Values are interpreted as tonnes/year
# =========================

waste_totals = {
    "ALTRO_RD": 670.853,
    "INGOMBRANTI_MISTI": 5341.740,
    "CARTA_CARTONE": 5065.937,
    "FRAZIONE_ORGANICA": 11892.390,
    "LEGNO": 1551.400,
    "METALLO": 255.547,
    "PLASTICA": 2126.822,
    "RAEE": 192.204,
    "SELETTIVA": 39.962,
    "TESSILI": 1596.350,
    "VETRO": 2569.613,
    "RIFIUTI_CD": 1122.220,
    "PULIZIA_STRADALE_RECUPERO": 0.0,
}


# =========================
# HELPERS
# =========================


def read_address_csv(path: str) -> pd.DataFrame:
    """
    Reads a CSV with automatic separator detection.
    Works with comma, semicolon, tab, or whitespace-separated files.
    """
    try:
        return pd.read_csv(path, sep=None, engine="python", dtype=str)
    except Exception:
        return pd.read_csv(path, delim_whitespace=True, dtype=str)


def normalize_numeric_column(series: pd.Series) -> pd.Series:
    """
    Converts Italian-style numeric text into floats.
    Handles values such as:
    - 5.341,740
    - 5341,740
    - 5341.740
    """
    s = series.astype(str).str.strip()

    # Remove spaces
    s = s.str.replace(" ", "", regex=False)

    # If both . and , exist, assume Italian format:
    # . = thousands separator, , = decimal separator
    mask_italian = s.str.contains(r"\.", regex=True) & s.str.contains(",", regex=False)
    s.loc[mask_italian] = (
        s.loc[mask_italian]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    # If only comma exists, treat comma as decimal separator
    mask_comma_decimal = ~mask_italian & s.str.contains(",", regex=False)
    s.loc[mask_comma_decimal] = s.loc[mask_comma_decimal].str.replace(
        ",", ".", regex=False
    )

    return pd.to_numeric(s, errors="coerce")


def compute_density_weights(df: pd.DataFrame) -> np.ndarray:
    """
    Computes a synthetic garbage-production weight for each point.

    The weight combines:
    - local point density within DENSITY_RADIUS
    - a small random factor
    """
    coords = df[[X_COL, Y_COL]].to_numpy(dtype=float)

    nbrs = NearestNeighbors(radius=DENSITY_RADIUS)
    nbrs.fit(coords)

    neighbors = nbrs.radius_neighbors(coords, return_distance=False)

    # Number of nearby points, including the point itself
    density = np.array([len(n) for n in neighbors], dtype=float)

    # Avoid zero weights
    density = np.maximum(density, 1.0)

    # Apply non-linear density effect
    weights = density**DENSITY_EXPONENT

    if RANDOM_NOISE > 0:
        rng = np.random.default_rng(RANDOM_SEED)
        noise = rng.lognormal(mean=0.0, sigma=RANDOM_NOISE, size=len(weights))
        weights = weights * noise

    weights = weights / weights.sum()

    return weights


# =========================
# MAIN
# =========================


def main():
    df = read_address_csv(INPUT_CSV)

    required_cols = [
        "CODICE_COMUNE",
        "CODICE_ISTAT",
        "PROGRESSIVO_NAZIONALE",
        "CODICE_COMUNALE",
        "ODONIMO",
        "LOCALITA'",
        "DIZIONE_LINGUA1",
        "DIZIONE_LINGUA2",
        "PROGRESSIVO_ACCESSO",
        "CODICE_COMUNALE_ACCESSO",
        "CIVICO",
        "ESPONENTE",
        "SPECIFICITA",
        "METRICO",
        "PROGRESSIVO_SNC",
        "COORD_X_COMUNE",
        "COORD_Y_COMUNE",
        "QUOTA",
        "METODO",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input CSV: {missing}")

    # Keep only the municipality of interest
    df["CODICE_ISTAT"] = df["CODICE_ISTAT"].astype(str).str.strip()
    city = df[df["CODICE_ISTAT"] == str(FIXED_CODICE_ISTAT)].copy()

    if city.empty:
        raise ValueError(
            f"No rows found with CODICE_ISTAT={FIXED_CODICE_ISTAT}. "
            "Check the value used in FIXED_CODICE_ISTAT."
        )

    # Convert coordinates
    city[X_COL] = normalize_numeric_column(city[X_COL])
    city[Y_COL] = normalize_numeric_column(city[Y_COL])

    city = city.dropna(subset=[X_COL, Y_COL]).copy()

    if city.empty:
        raise ValueError("No valid coordinate rows found after numeric conversion.")

    # Compute spatial weights
    weights = compute_density_weights(city)

    # Add synthetic waste production columns
    for waste_name, total_tonnes in waste_totals.items():
        city[f"{waste_name}_TONNES_YEAR"] = weights * total_tonnes
        city[f"{waste_name}_KG_YEAR"] = city[f"{waste_name}_TONNES_YEAR"] * 1000

    total_tonnes = sum(waste_totals.values())
    city["TOTAL_GARBAGE_TONNES_YEAR"] = weights * total_tonnes
    city["TOTAL_GARBAGE_KG_YEAR"] = city["TOTAL_GARBAGE_TONNES_YEAR"] * 1000

    # Useful heatmap intensity value
    city["HEATMAP_WEIGHT"] = weights
    city["LOCAL_POINT_DENSITY_RADIUS"] = (
        city["HEATMAP_WEIGHT"] / city["HEATMAP_WEIGHT"].mean()
    )

    # Force CODICE_ISTAT to be fixed and equal for every output row
    city["CODICE_ISTAT"] = str(FIXED_CODICE_ISTAT)

    output_columns = required_cols + [
        "HEATMAP_WEIGHT",
        "LOCAL_POINT_DENSITY_RADIUS",
        "TOTAL_GARBAGE_TONNES_YEAR",
        "TOTAL_GARBAGE_KG_YEAR",
    ]

    for waste_name in waste_totals:
        output_columns.append(f"{waste_name}_TONNES_YEAR")
        output_columns.append(f"{waste_name}_KG_YEAR")

    city[output_columns].to_csv(OUTPUT_CSV, index=False)

    print(f"Created: {OUTPUT_CSV}")
    print(f"Rows: {len(city)}")
    print(f"Total tonnes distributed: {city['TOTAL_GARBAGE_TONNES_YEAR'].sum():.3f}")
    print(f"Total kg distributed: {city['TOTAL_GARBAGE_KG_YEAR'].sum():.3f}")


if __name__ == "__main__":
    main()
