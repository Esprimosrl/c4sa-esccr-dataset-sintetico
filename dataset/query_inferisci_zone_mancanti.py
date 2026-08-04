#!/usr/bin/env python3

import argparse
from pathlib import Path

import duckdb

try:
    from dataset import config
except ModuleNotFoundError:
    import config


def sql_path(path: str) -> str:
    return "'" + Path(path).as_posix().replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inferisce la zona mancante dal centroide piu vicino.")
    parser.add_argument("--in", dest="input", default=config.ANNUAL_ZONE_CSV)
    parser.add_argument("--out", default=config.ANNUAL_ZONE_INFERRED_CSV)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    duckdb.connect().execute(
        f"""
        COPY (
            WITH my_table AS (
                SELECT
                    row_number() OVER () AS rid,
                    *,
                    zona AS zona_clean
                FROM read_csv_auto({sql_path(args.input)}, sample_size = -1)
            ),
            centroids AS (
                SELECT
                    zona_clean AS zona,
                    avg(coord_x) AS cx,
                    avg(coord_y) AS cy
                FROM my_table
                WHERE zona_clean IS NOT NULL
                GROUP BY zona_clean
            ),
            nearest AS (
                SELECT
                    t.rid,
                    c.zona AS inferred_zona,
                    sqrt(
                        pow(t.coord_x - c.cx, 2) +
                        pow(t.coord_y - c.cy, 2)
                    ) AS distance_to_centroid
                FROM my_table t
                CROSS JOIN centroids c
                WHERE t.zona_clean IS NULL
                QUALIFY row_number() OVER (
                    PARTITION BY t.rid
                    ORDER BY distance_to_centroid
                ) = 1
            )
            SELECT
                t.* EXCLUDE (rid, zona, zona_clean),
                t.zona AS zona_raccolta_originale,
                coalesce(t.zona_clean, n.inferred_zona) AS zona,
                coalesce(t.zona_clean, n.inferred_zona) AS zona_raccolta,
                t.zona_clean IS NULL AS zona_raccolta_inferita,
                n.distance_to_centroid AS distanza_inferenza_zona_raccolta_gradi
            FROM my_table t
            LEFT JOIN nearest n USING (rid)
        ) TO {sql_path(args.out)} (HEADER, DELIMITER ',')
        """
    )


if __name__ == "__main__":
    main()
