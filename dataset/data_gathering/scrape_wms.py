#!/usr/bin/env python3

import argparse
import csv
import json
import os
import random
import time
from pathlib import Path

import requests
from shapely.geometry import Point, shape, mapping
from shapely.errors import GEOSException
from shapely.strtree import STRtree


BASE_URL = "https://comune-andria.planetek.it/ows/3/qdjango/6/"
LAYER = "prg_andria_v4_7b29ddc5_9192_4576_b367_430a4d102c81"


def parse_italian_number(value):
    """
    Parse numbers like:
      16,1385014
      590.933,813
      4.540.389,481
      16.1385014

    Italian format:
      . = thousands separator
      , = decimal separator
    """
    if value is None:
        raise ValueError("Missing numeric value")

    s = str(value).strip()

    if s == "":
        raise ValueError("Empty numeric value")

    # Normalize common weird characters
    s = (
        s.replace("\u00a0", "")  # non-breaking space
        .replace(" ", "")
        .replace("−", "-")  # unicode minus
    )

    # Italian format: 1.234.567,89
    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
        return float(s)

    # Already standard decimal format: 1234.56
    return float(s)


def default_delta_for_crs(crs: str) -> float:
    if crs.upper() in {"CRS:84", "EPSG:4326"}:
        return 0.0005  # degrees
    return 5.0  # metres, e.g. EPSG:32633


def request_feature_info(x, y, crs, delta, session, max_retries=3):
    params = {
        "SERVICE": "WMS",
        "VERSION": "1.3.0",
        "REQUEST": "GetFeatureInfo",
        "LAYERS": LAYER,
        "QUERY_LAYERS": LAYER,
        "STYLES": "predefinito",
        "CRS": crs,
        "BBOX": f"{x - delta},{y - delta},{x + delta},{y + delta}",
        "WIDTH": 101,
        "HEIGHT": 101,
        "I": 50,
        "J": 50,
        "INFO_FORMAT": "application/geo+json",
        "FEATURE_COUNT": 20,
        "WITH_GEOMETRY": "TRUE",
    }

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(BASE_URL, params=params, timeout=45)
            r.raise_for_status()
            data = r.json()

            if data.get("type") != "FeatureCollection":
                raise RuntimeError(f"Unexpected response: {str(data)[:300]}")

            return data.get("features", [])

        except Exception as e:
            last_error = e
            time.sleep(1.5 * attempt)

    raise RuntimeError(f"Request failed after {max_retries} attempts: {last_error}")


def feature_key(feature):
    props = feature.get("properties") or {}

    return str(
        props.get("gid")
        or props.get("id")
        or feature.get("id")
        or json.dumps(feature.get("geometry"), sort_keys=True)
    )


def safe_shape(feature):
    geom = feature.get("geometry")

    if not geom:
        return None

    try:
        return shape(geom)
    except GEOSException:
        return None


def props_to_output_fields(props):
    return {
        "prg_codice_zona": props.get("Codice zona"),
        "prg_zona": props.get("Zona"),
        "prg_norme": props.get("Norme"),
        "prg_normative": props.get("normative"),
        "prg_fonte": props.get("Fonte"),
        "prg_gid": props.get("gid"),
        "prg_id": props.get("id"),
    }


def output_row(point):
    props = point["props"] or {}

    out = dict(point["row"])
    out.update(props_to_output_fields(props))
    out["prg_feature_id"] = point["feature_id"]
    out["prg_error"] = point["error"]

    return out


def existing_output_rows(path, expected_fields):
    if not path.exists() or path.stat().st_size == 0:
        return 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            return 0

        if reader.fieldnames != expected_fields:
            raise RuntimeError(f"Existing output CSV header does not match: {path}")

        return sum(1 for _ in reader)


def write_polygons(path, known_features, known_geometries):
    polygon_features = []

    for key, feature in known_features.items():
        props = feature.get("properties") or {}
        geom = known_geometries[key]

        polygon_features.append(
            {
                "type": "Feature",
                "id": key,
                "properties": props,
                "geometry": mapping(geom),
            }
        )

    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "type": "FeatureCollection",
                "features": polygon_features,
            },
            f,
            ensure_ascii=False,
        )


def load_polygons(path):
    if not path.exists() or path.stat().st_size == 0:
        return {}, {}

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    known_features = {}
    known_geometries = {}

    for feature in data.get("features", []):
        key = str(feature.get("id") or feature_key(feature))
        geom = safe_shape(feature)

        if geom is None:
            continue

        known_features[key] = feature
        known_geometries[key] = geom

    return known_features, known_geometries


def sleep_around(seconds):
    if seconds > 0:
        time.sleep(random.uniform(seconds * 0.5, seconds * 1.5))


def candidate_key(hit, keys, geoms, geom_keys):
    if hasattr(hit, "__index__"):
        return keys[hit.__index__()]

    return geom_keys[id(hit)]


def classify_from_polygons(points, known_features, known_geometries, stream_ready_rows):
    if not known_geometries:
        raise RuntimeError("Polygon source did not contain usable geometries.")

    keys = list(known_geometries)
    geoms = [known_geometries[key] for key in keys]
    geom_keys = {id(geom): key for key, geom in zip(keys, geoms)}
    tree = STRtree(geoms)
    classified = 0

    for p in points:
        if p["classified"]:
            continue

        match_key = None

        for hit in tree.query(p["point"]):
            key = candidate_key(hit, keys, geoms, geom_keys)

            if known_geometries[key].covers(p["point"]):
                match_key = key
                break

        p["classified"] = True

        if match_key:
            p["props"] = known_features[match_key].get("properties") or {}
            p["feature_id"] = match_key
        else:
            p["props"] = {}
            p["error"] = "No polygon contains point"

        classified += 1
        stream_ready_rows()

        if classified % 1000 == 0:
            print(f"Local polygon classification: +{classified}")

    return classified


def main():
    parser = argparse.ArgumentParser(
        description="Classify CSV points using the Andria PRG WMS with the fewest practical GetFeatureInfo requests."
    )

    parser.add_argument("input_csv", help="Input CSV containing point coordinates.")
    parser.add_argument("output_csv", help="Output CSV with PRG fields added.")
    parser.add_argument(
        "--x-field",
        default="X",
        help="CSV column containing X / longitude. Default: X",
    )
    parser.add_argument(
        "--y-field",
        default="Y",
        help="CSV column containing Y / latitude. Default: Y",
    )
    parser.add_argument(
        "--crs",
        default="CRS:84",
        help="CRS of the CSV coordinates and WMS query. Use CRS:84 for lon/lat, EPSG:32633 for UTM 33N. Default: CRS:84",
    )
    parser.add_argument(
        "--delta",
        type=float,
        default=None,
        help="Half-size of the GetFeatureInfo BBOX. Default: 0.0005 for CRS:84, 5 for projected CRS.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.05,
        help="Average sleep seconds between HTTP requests; actual wait is random around it. Default: 0.05",
    )
    parser.add_argument(
        "--polygons-out",
        default="retrieved_prg_polygons.geojson",
        help="GeoJSON output containing unique retrieved polygons.",
    )
    parser.add_argument(
        "--polygons-source",
        default=None,
        help="Classify locally from this GeoJSON instead of querying WMS.",
    )

    args = parser.parse_args()

    delta = args.delta if args.delta is not None else default_delta_for_crs(args.crs)

    with open(args.input_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=","))

    if not rows:
        raise RuntimeError("Input CSV is empty.")

    for field in [args.x_field, args.y_field]:
        if field not in rows[0]:
            raise RuntimeError(f"Missing coordinate field: {field}")

    points = []

    for idx, row in enumerate(rows):
        x = parse_italian_number(row[args.x_field])
        y = parse_italian_number(row[args.y_field])
        points.append(
            {
                "idx": idx,
                "row": row,
                "x": x,
                "y": y,
                "point": Point(x, y),
                "classified": False,
                "props": None,
                "feature_id": None,
                "error": None,
            }
        )

    extra_fields = [
        "prg_codice_zona",
        "prg_zona",
        "prg_norme",
        "prg_normative",
        "prg_fonte",
        "prg_gid",
        "prg_id",
        "prg_feature_id",
        "prg_error",
    ]

    input_fields = list(rows[0].keys())
    output_fields = input_fields + [f for f in extra_fields if f not in input_fields]
    output_path = Path(args.output_csv)
    polygons_path = Path(args.polygons_out)
    # ponytail: resume by completed output row count; use stable row IDs if input order can change.
    resume_count = existing_output_rows(output_path, output_fields)

    if resume_count > len(points):
        raise RuntimeError(
            f"Existing output has {resume_count} rows, but input only has {len(points)} rows."
        )

    for p in points[:resume_count]:
        p["classified"] = True

    session = requests.Session()

    source_path = Path(args.polygons_source) if args.polygons_source else polygons_path
    known_features, known_geometries = load_polygons(source_path)

    request_count = 0
    classified_count = resume_count
    next_to_write = resume_count

    with open(
        output_path, "a" if resume_count else "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)

        if not resume_count:
            writer.writeheader()
            f.flush()
            os.fsync(f.fileno())

        def stream_ready_rows():
            nonlocal next_to_write

            while next_to_write < len(points) and points[next_to_write]["classified"]:
                writer.writerow(output_row(points[next_to_write]))
                next_to_write += 1

            f.flush()
            os.fsync(f.fileno())

        if args.polygons_source:
            classified_count += classify_from_polygons(
                points,
                known_features,
                known_geometries,
                stream_ready_rows,
            )
            print(
                f"Local polygon classification complete. "
                f"Progress: {classified_count}/{len(points)}"
            )
        else:
            while True:
                remaining = [p for p in points if not p["classified"]]

                if not remaining:
                    break

                seed = remaining[0]

                try:
                    features = request_feature_info(
                        seed["x"],
                        seed["y"],
                        args.crs,
                        delta,
                        session,
                    )
                    request_count += 1
                except Exception as e:
                    seed["classified"] = True
                    seed["props"] = {}
                    seed["error"] = str(e)
                    classified_count += 1
                    stream_ready_rows()
                    print(
                        f"Request error for row {seed['idx']}: {e}. "
                        f"Progress: {classified_count}/{len(points)}"
                    )
                    continue

                if not features:
                    seed["classified"] = True
                    seed["props"] = {}
                    seed["error"] = "No WMS feature returned"
                    classified_count += 1
                    stream_ready_rows()
                    print(
                        f"No feature for row {seed['idx']}. "
                        f"Progress: {classified_count}/{len(points)}"
                    )
                    sleep_around(args.sleep)
                    continue

                new_feature_added = False

                for feature in features:
                    key = feature_key(feature)
                    geom = safe_shape(feature)

                    if geom is None:
                        continue

                    if key not in known_features:
                        known_features[key] = feature
                        known_geometries[key] = geom
                        new_feature_added = True

                if new_feature_added:
                    write_polygons(polygons_path, known_features, known_geometries)

                if not new_feature_added:
                    # Fallback: classify only the seed from the first returned feature.
                    feature = features[0]
                    props = feature.get("properties") or {}

                    seed["classified"] = True
                    seed["props"] = props
                    seed["feature_id"] = feature_key(feature)
                    classified_count += 1
                    stream_ready_rows()

                    print(
                        f"Seed-only classification for row {seed['idx']}. "
                        f"Progress: {classified_count}/{len(points)}. "
                        f"Requests: {request_count}"
                    )
                    sleep_around(args.sleep)
                    continue

                # Re-scan all unclassified points against all known polygons.
                newly_classified = 0

                for p in points:
                    if p["classified"]:
                        continue

                    for key, geom in known_geometries.items():
                        # covers() includes boundary points; contains() would exclude boundary.
                        if geom.covers(p["point"]):
                            props = known_features[key].get("properties") or {}

                            p["classified"] = True
                            p["props"] = props
                            p["feature_id"] = key

                            newly_classified += 1
                            classified_count += 1
                            stream_ready_rows()
                            break

                print(
                    f"Request {request_count}: found {len(known_features)} unique polygons; "
                    f"classified +{newly_classified}; "
                    f"progress {classified_count}/{len(points)}"
                )

                sleep_around(args.sleep)

    if not args.polygons_source:
        write_polygons(polygons_path, known_features, known_geometries)

    print("")
    print(f"Done.")
    print(f"Input points: {len(points)}")
    print(f"WMS requests: {request_count}")
    print(f"Polygons loaded: {len(known_features)}")
    print(f"Output CSV: {args.output_csv}")
    print(f"Polygon source: {source_path}")
    if not args.polygons_source:
        print(f"Output polygons: {args.polygons_out}")


if __name__ == "__main__":
    main()
