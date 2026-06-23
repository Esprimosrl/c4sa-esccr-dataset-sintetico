#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

import polars as pl


X_FIELD = "COORD_X_COMUNE"
Y_FIELD = "COORD_Y_COMUNE"
FILTER_FIELD = "CODICE_ISTAT"
FILTER_VALUE = "110001"


def detect_separator(csv_path: Path) -> str:
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        header = f.readline()

    candidates = ["\t", ",", ";"]
    return max(candidates, key=header.count)


def build_lazy_frame(input_csv: Path, separator: str) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            input_csv,
            separator=separator,
            encoding="utf8-lossy",
            infer_schema_length=0,
            ignore_errors=True,
        )
        .filter(pl.col(FILTER_FIELD).str.strip_chars() == FILTER_VALUE)
        .with_columns(
            pl.col(X_FIELD)
            .str.strip_chars()
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
            .alias("_x"),
            pl.col(Y_FIELD)
            .str.strip_chars()
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
            .alias("_y"),
        )
        .filter(pl.col("_x").is_not_null() & pl.col("_y").is_not_null())
    )


def row_to_feature(row: dict, property_fields: list[str]) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row["_x"], row["_y"]],
        },
        "properties": {field: row[field] for field in property_fields},
    }


def csv_to_geojson_streaming(
    input_csv: Path,
    output_geojson: Path,
    batch_size: int,
) -> None:
    separator = detect_separator(input_csv)
    lazy_df = build_lazy_frame(input_csv, separator)

    schema_names = lazy_df.collect_schema().names()

    required_fields = {X_FIELD, Y_FIELD, FILTER_FIELD}
    missing_fields = required_fields - set(schema_names)

    if missing_fields:
        raise ValueError(
            f"Missing required fields: {sorted(missing_fields)}. "
            f"Found fields: {schema_names}"
        )

    property_fields = [col for col in schema_names if col not in {X_FIELD, Y_FIELD}]

    written = 0

    with open(output_geojson, "w", encoding="utf-8") as f:
        f.write('{"type":"FeatureCollection","features":[')

        first_feature = True

        for batch in lazy_df.collect_batches(chunk_size=batch_size):
            for row in batch.iter_rows(named=True):
                feature = row_to_feature(row, property_fields)

                if not first_feature:
                    f.write(",")
                else:
                    first_feature = False

                json.dump(feature, f, ensure_ascii=False, separators=(",", ":"))
                written += 1

        f.write("]}")

    print(f"Created: {output_geojson}")
    print(f"Features written: {written}")
    print(f"Detected separator: {repr(separator)}")
    print(f"Batch size: {batch_size}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream a filtered CSV to GeoJSON using Polars."
    )
    parser.add_argument("input_csv", help="Input CSV file")
    parser.add_argument("output_geojson", help="Output GeoJSON file")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
        help="Number of rows per Polars batch. Default: 50000",
    )

    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    output_geojson = Path(args.output_geojson)

    if not input_csv.exists():
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    csv_to_geojson_streaming(
        input_csv=input_csv,
        output_geojson=output_geojson,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
