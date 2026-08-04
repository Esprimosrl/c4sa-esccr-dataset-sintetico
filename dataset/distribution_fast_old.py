from pathlib import Path
import polars as pl


# ============================================================
# CONFIG
# ============================================================

INPUT_CSV = "INDIR_PUGL_20260602.csv"
OUTPUT_CSV = "andria_synthetic_garbage_distribution.csv"

# Replace with the actual CODICE_ISTAT found in your address file.
# Andria is commonly 110001, but verify your source file.
FIXED_CODICE_ISTAT = "110001"

X_COL = "COORD_X_COMUNE"
Y_COL = "COORD_Y_COMUNE"

# If coordinates are projected in meters:
# 100.0 means each grid cell is 100m x 100m.
# Try 50, 100, 150, 250, 500 depending on how smooth you want the heatmap.
GRID_SIZE = 1.0

# Neighborhood radius in grid cells.
# 1 = 3x3 cells
# 2 = 5x5 cells
# 3 = 7x7 cells
NEIGHBORHOOD_CELLS = 3

# Controls concentration of garbage in dense areas.
# 1.0 = linear density
# >1.0 = dense areas get more weight
# <1.0 = smoother distribution
DENSITY_EXPONENT = 1.15

# Output CSV separator.
OUTPUT_SEPARATOR = ","


# ============================================================
# INPUT COLUMNS
# ============================================================

REQUIRED_COLUMNS = [
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


# ============================================================
# MUNICIPAL WASTE TOTALS
# Tonnes/year
# ============================================================

WASTE_TOTALS_TONNES = {
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


# ============================================================
# HELPERS
# ============================================================


def scan_csv_auto(path: str) -> pl.LazyFrame:
    """
    Tries common CSV separators and returns a LazyFrame.

    If your input file is definitely comma/semicolon/tab separated,
    you can simplify this and call pl.scan_csv directly.
    """
    separators = [";", ",", "\t", "|"]

    last_error = None

    for sep in separators:
        try:
            lf = pl.scan_csv(
                path,
                separator=sep,
                infer_schema_length=0,
                ignore_errors=True,
                truncate_ragged_lines=True,
                null_values=["", "NULL", "null", "None"],
            )

            columns = lf.collect_schema().names()

            if "CODICE_ISTAT" in columns and X_COL in columns and Y_COL in columns:
                print(f"Detected separator: {repr(sep)}")
                return lf

        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Could not detect CSV separator. Last error was: {last_error}")


def italian_number_expr(column_name: str) -> pl.Expr:
    """
    Converts Italian-style numeric strings into Float64.

    Examples:
    - '5.341,740' -> 5341.740
    - '5341,740'  -> 5341.740
    - '5341.740'  -> 5341.740
    """
    c = pl.col(column_name).cast(pl.Utf8).str.strip_chars()

    has_dot = c.str.contains(r"\.")
    has_comma = c.str.contains(",")

    return (
        pl.when(has_dot & has_comma)
        .then(c.str.replace_all(r"\.", "").str.replace(",", "."))
        .otherwise(c.str.replace(",", "."))
        .cast(pl.Float64, strict=False)
    )


def make_neighbor_offsets(radius_cells: int) -> pl.LazyFrame:
    """
    Builds offsets for a square neighborhood.

    radius_cells = 1 -> 3x3
    radius_cells = 2 -> 5x5
    radius_cells = 3 -> 7x7
    """
    offsets = [
        {"DX": dx, "DY": dy}
        for dx in range(-radius_cells, radius_cells + 1)
        for dy in range(-radius_cells, radius_cells + 1)
    ]

    return pl.DataFrame(offsets).lazy()


def validate_density(debug: pl.DataFrame) -> None:
    """
    Prints a warning if the density is still flat.
    """
    min_density = debug["min_neighborhood_count"][0]
    max_density = debug["max_neighborhood_count"][0]

    if min_density == max_density:
        print()
        print("WARNING: Density is still flat.")
        print("All points have the same NEIGHBORHOOD_POINT_COUNT.")
        print("Try changing GRID_SIZE or NEIGHBORHOOD_CELLS.")
        print("Examples:")
        print("  GRID_SIZE = 50.0,  NEIGHBORHOOD_CELLS = 2")
        print("  GRID_SIZE = 100.0, NEIGHBORHOOD_CELLS = 2")
        print("  GRID_SIZE = 250.0, NEIGHBORHOOD_CELLS = 1")
        print()


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    input_path = Path(INPUT_CSV)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    lf = scan_csv_auto(str(input_path))

    available_columns = lf.collect_schema().names()
    missing_columns = [c for c in REQUIRED_COLUMNS if c not in available_columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # ------------------------------------------------------------
    # Base lazy dataset
    # ------------------------------------------------------------

    base = (
        lf.select(REQUIRED_COLUMNS)
        .with_columns(
            pl.col("CODICE_ISTAT").cast(pl.Utf8).str.strip_chars(),
            italian_number_expr(X_COL).alias(X_COL),
            italian_number_expr(Y_COL).alias(Y_COL),
        )
        .filter(pl.col("CODICE_ISTAT") == str(FIXED_CODICE_ISTAT))
        .filter(pl.col(X_COL).is_not_null() & pl.col(Y_COL).is_not_null())
        .with_row_index("POINT_ID")
        .with_columns(
            (pl.col(X_COL) / GRID_SIZE).floor().cast(pl.Int64).alias("GRID_X"),
            (pl.col(Y_COL) / GRID_SIZE).floor().cast(pl.Int64).alias("GRID_Y"),
        )
    )

    # ------------------------------------------------------------
    # Count points per grid cell
    # ------------------------------------------------------------

    cell_density = base.group_by(["GRID_X", "GRID_Y"]).agg(
        pl.len().alias("CELL_POINT_COUNT")
    )

    # ------------------------------------------------------------
    # For every point, build a neighborhood of nearby grid cells
    # ------------------------------------------------------------

    neighbor_offsets = make_neighbor_offsets(NEIGHBORHOOD_CELLS)

    point_neighbor_cells = (
        base.select(["POINT_ID", "GRID_X", "GRID_Y"])
        .join(neighbor_offsets, how="cross")
        .with_columns(
            (pl.col("GRID_X") + pl.col("DX")).alias("NEIGHBOR_GRID_X"),
            (pl.col("GRID_Y") + pl.col("DY")).alias("NEIGHBOR_GRID_Y"),
        )
        .select(
            [
                "POINT_ID",
                "NEIGHBOR_GRID_X",
                "NEIGHBOR_GRID_Y",
            ]
        )
    )

    neighborhood_density = (
        point_neighbor_cells.join(
            cell_density,
            left_on=["NEIGHBOR_GRID_X", "NEIGHBOR_GRID_Y"],
            right_on=["GRID_X", "GRID_Y"],
            how="left",
        )
        .with_columns(pl.col("CELL_POINT_COUNT").fill_null(0))
        .group_by("POINT_ID")
        .agg(pl.col("CELL_POINT_COUNT").sum().alias("NEIGHBORHOOD_POINT_COUNT"))
    )

    # ------------------------------------------------------------
    # Assign raw density weights
    # ------------------------------------------------------------

    enriched = (
        base.join(neighborhood_density, on="POINT_ID", how="left")
        .with_columns(
            pl.col("NEIGHBORHOOD_POINT_COUNT")
            .fill_null(1)
            .cast(pl.Float64)
            .alias("NEIGHBORHOOD_POINT_COUNT")
        )
        .with_columns(
            (pl.col("NEIGHBORHOOD_POINT_COUNT") ** DENSITY_EXPONENT).alias("RAW_WEIGHT")
        )
    )

    # ------------------------------------------------------------
    # Diagnostics before normalization
    # ------------------------------------------------------------

    debug = enriched.select(
        pl.len().alias("rows"),
        pl.col(X_COL).min().alias("min_x"),
        pl.col(X_COL).max().alias("max_x"),
        pl.col(Y_COL).min().alias("min_y"),
        pl.col(Y_COL).max().alias("max_y"),
        pl.col("GRID_X").n_unique().alias("unique_grid_x"),
        pl.col("GRID_Y").n_unique().alias("unique_grid_y"),
        pl.col("NEIGHBORHOOD_POINT_COUNT").min().alias("min_neighborhood_count"),
        pl.col("NEIGHBORHOOD_POINT_COUNT").max().alias("max_neighborhood_count"),
        pl.col("NEIGHBORHOOD_POINT_COUNT").mean().alias("mean_neighborhood_count"),
        pl.col("RAW_WEIGHT").min().alias("min_raw_weight"),
        pl.col("RAW_WEIGHT").max().alias("max_raw_weight"),
        pl.col("RAW_WEIGHT").sum().alias("raw_weight_sum"),
    ).collect(streaming=True)

    print()
    print("Density diagnostics:")
    print(debug)
    validate_density(debug)

    rows = debug["rows"][0]
    raw_weight_sum = debug["raw_weight_sum"][0]

    if rows == 0:
        raise ValueError(
            f"No rows found for CODICE_ISTAT={FIXED_CODICE_ISTAT}. "
            "Check FIXED_CODICE_ISTAT and input data."
        )

    if raw_weight_sum is None or raw_weight_sum <= 0:
        raise ValueError("Invalid RAW_WEIGHT sum. Cannot distribute totals.")

    # ------------------------------------------------------------
    # Normalize weights and distribute municipal totals
    # ------------------------------------------------------------

    total_tonnes = sum(WASTE_TOTALS_TONNES.values())

    result = enriched.with_columns(
        (pl.col("RAW_WEIGHT") / raw_weight_sum).alias("HEATMAP_WEIGHT"),
        (
            pl.col("NEIGHBORHOOD_POINT_COUNT")
            / pl.col("NEIGHBORHOOD_POINT_COUNT").mean()
        ).alias("LOCAL_POINT_DENSITY_INDEX"),
        (pl.col("RAW_WEIGHT") / raw_weight_sum * total_tonnes).alias(
            "TOTAL_GARBAGE_TONNES_YEAR"
        ),
        (pl.col("RAW_WEIGHT") / raw_weight_sum * total_tonnes * 1000).alias(
            "TOTAL_GARBAGE_KG_YEAR"
        ),
        pl.lit(str(FIXED_CODICE_ISTAT)).alias("CODICE_ISTAT"),
    )

    for waste_name, tonnes in WASTE_TOTALS_TONNES.items():
        result = result.with_columns(
            (pl.col("HEATMAP_WEIGHT") * tonnes).alias(f"{waste_name}_TONNES_YEAR"),
            (pl.col("HEATMAP_WEIGHT") * tonnes * 1000).alias(f"{waste_name}_KG_YEAR"),
        )

    output_columns = (
        REQUIRED_COLUMNS
        + [
            "POINT_ID",
            "GRID_X",
            "GRID_Y",
            "NEIGHBORHOOD_POINT_COUNT",
            "LOCAL_POINT_DENSITY_INDEX",
            "HEATMAP_WEIGHT",
            "TOTAL_GARBAGE_TONNES_YEAR",
            "TOTAL_GARBAGE_KG_YEAR",
        ]
        + [
            column
            for waste_name in WASTE_TOTALS_TONNES
            for column in [
                f"{waste_name}_TONNES_YEAR",
                f"{waste_name}_KG_YEAR",
            ]
        ]
    )

    # ------------------------------------------------------------
    # Write CSV
    # ------------------------------------------------------------

    (
        result.select(output_columns).sink_csv(
            OUTPUT_CSV,
            separator=OUTPUT_SEPARATOR,
        )
    )

    # ------------------------------------------------------------
    # Validation pass
    # ------------------------------------------------------------

    check = (
        pl.scan_csv(OUTPUT_CSV)
        .select(
            pl.len().alias("rows"),
            pl.col("HEATMAP_WEIGHT").sum().alias("heatmap_weight_sum"),
            pl.col("TOTAL_GARBAGE_TONNES_YEAR").sum().alias("total_tonnes"),
            pl.col("TOTAL_GARBAGE_KG_YEAR").sum().alias("total_kg"),
            pl.col("TOTAL_GARBAGE_KG_YEAR").min().alias("min_kg_per_point"),
            pl.col("TOTAL_GARBAGE_KG_YEAR").max().alias("max_kg_per_point"),
        )
        .collect(streaming=True)
    )

    print()
    print("Output validation:")
    print(check)

    print()
    print(f"Created: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
