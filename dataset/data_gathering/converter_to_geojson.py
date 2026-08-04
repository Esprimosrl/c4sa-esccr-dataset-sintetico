#!/usr/bin/env python3

import csv
import json
import argparse
from pathlib import Path


X_FIELD = "COORD_X_COMUNE"
Y_FIELD = "COORD_Y_COMUNE"
FILTER_FIELD = "CODICE_ISTAT"
FILTER_VALUE = "110001"


def parse_float(value):
    if value is None:
        return None

    value = value.strip()
    if not value:
        return None

    value = value.replace(",", ".")

    try:
        return float(value)
    except ValueError:
        return None


def detect_dialect(csv_path):
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)

    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
        dialect.delimiter = ","
        return dialect


def csv_to_geojson(input_csv, output_geojson):
    dialect = detect_dialect(input_csv)

    features = []
    skipped_coordinates = 0
    skipped_filter = 0

    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)

        required_fields = {X_FIELD, Y_FIELD, FILTER_FIELD}
        missing_fields = required_fields - set(reader.fieldnames or [])

        if missing_fields:
            raise ValueError(
                f"Campi obbligatori mancanti: {sorted(missing_fields)}. "
                f"Campi trovati: {reader.fieldnames}"
            )

        for row in reader:
            codice_istat = (row.get(FILTER_FIELD) or "").strip()

            if codice_istat != FILTER_VALUE:
                skipped_filter += 1
                continue

            x = parse_float(row.get(X_FIELD))
            y = parse_float(row.get(Y_FIELD))

            if x is None or y is None:
                skipped_coordinates += 1
                continue

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [x, y],
                },
                "properties": {
                    key: value
                    for key, value in row.items()
                    if key not in (X_FIELD, Y_FIELD)
                },
            }

            features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    with open(output_geojson, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"Creato: {output_geojson}")
    print(f"Feature scritte: {len(features)}")
    print(f"Righe escluse per CODICE_ISTAT diverso da {FILTER_VALUE}: {skipped_filter}")
    print(f"Righe escluse per coordinate mancanti/non valide: {skipped_coordinates}")


def main():
    parser = argparse.ArgumentParser(
        description="Converte un CSV filtrato per CODICE_ISTAT in GeoJSON."
    )
    parser.add_argument("input_csv", help="File CSV di input")
    parser.add_argument("output_geojson", help="File GeoJSON di output")

    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    output_geojson = Path(args.output_geojson)

    if not input_csv.exists():
        raise FileNotFoundError(f"File di input non trovato: {input_csv}")

    csv_to_geojson(input_csv, output_geojson)


if __name__ == "__main__":
    main()
