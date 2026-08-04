#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dataset import config

BASE_URL = config.WMS_BASE_URL
LAYER = config.WMS_LAYER

params = {
    "SERVICE": "WMS",
    "VERSION": "1.3.0",
    "REQUEST": "GetFeatureInfo",
    "LAYERS": LAYER,
    "QUERY_LAYERS": LAYER,
    "STYLES": "predefinito",
    "CRS": "CRS:84",
    "BBOX": "16.081386,41.006687,16.364964,41.273536",
    "WIDTH": 1000,
    "HEIGHT": 1000,
    "INFO_FORMAT": "application/geo+json",
    "FEATURE_COUNT": 100000,
    "WITH_GEOMETRY": "TRUE",
    "FILTER": f'{LAYER}:"gid" IS NOT NULL',
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scarica tutte le zone PRG Andria dal WMS.")
    parser.add_argument("--out", default=config.FULL_WMS_ZONE_GEOJSON)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    r = requests.get(BASE_URL, params=params, timeout=120)
    r.raise_for_status()

    data = r.json()
    features = data.get("features", [])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"Downloaded {len(features)} features to {out}")


if __name__ == "__main__":
    main()
