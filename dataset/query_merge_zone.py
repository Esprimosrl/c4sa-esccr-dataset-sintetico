#!/usr/bin/env python3

import argparse
from pathlib import Path

import duckdb


def sql_path(path: str) -> str:
    return "'" + Path(path).as_posix().replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unisce la distribuzione annua alle zone di raccolta.")
    parser.add_argument("--annuo", default="data_csv_parquet/andria_distribuzione_annuale.csv")
    parser.add_argument("--stradario", default="data_csv_parquet/andria_stradario_zone.csv")
    parser.add_argument("--out", default="data_csv_parquet/andria_distribuzione_annuale_zone.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    duckdb.connect().execute(
        f"""
        COPY (
            WITH rif AS (
                SELECT *
                FROM read_csv_auto({sql_path(args.annuo)}, sample_size = -1)
            ),
            strad AS (
                SELECT *
                FROM read_csv_auto({sql_path(args.stradario)})
            ),
            strad_one AS (
                SELECT *
                FROM strad
                QUALIFY row_number() OVER (
                    PARTITION BY strada
                    ORDER BY strada
                ) = 1
            )
            SELECT rif.*, strad_one.zona
            FROM rif
            LEFT JOIN strad_one
                ON rif.odonimo = strad_one.strada
        ) TO {sql_path(args.out)} (HEADER, DELIMITER ',')
        """
    )


if __name__ == "__main__":
    main()
