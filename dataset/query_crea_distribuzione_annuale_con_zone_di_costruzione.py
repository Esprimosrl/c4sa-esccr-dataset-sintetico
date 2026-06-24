#!/usr/bin/env python3

import argparse
from pathlib import Path

import duckdb


def sql_path(path: str) -> str:
    return "'" + Path(path).as_posix().replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggiunge la zona di costruzione alla distribuzione annua."
    )
    parser.add_argument(
        "--annuo",
        default="data_csv_parquet/andria_distribuzione_annuale_zone_inferite.csv",
    )
    parser.add_argument(
        "--civici-zone",
        default="data_csv_parquet/civici_andria_classificati_per_zone_di_costruzione.csv ",
    )
    parser.add_argument(
        "--out",
        default="data_csv_parquet/andria_distribuzione_annuale_zone_inferite_e_zone_costruzione.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    duckdb.connect().execute(
        f"""
        COPY (
            SELECT
                civ_ann.*,
                civ_zona.prg_codice_zona_new AS zona_costruzione
            FROM read_csv_auto({sql_path(args.annuo)}) AS civ_ann
            LEFT JOIN read_csv_auto({sql_path(args.civici_zone)}) AS civ_zona
                ON civ_ann.id_civico = civ_zona.PROGRESSIVO_ACCESSO
        ) TO {sql_path(args.out)} (HEADER, DELIMITER ',')
        """
    )


if __name__ == "__main__":
    main()
