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


def copy_options(path: str) -> str:
    if Path(path).suffix.lower() == ".parquet":
        return "FORMAT PARQUET, COMPRESSION ZSTD"
    return "HEADER, DELIMITER ','"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggiunge la zona di costruzione alla distribuzione annua."
    )
    parser.add_argument(
        "--annuo",
        default=config.ANNUAL_ZONE_INFERRED_CSV,
    )
    parser.add_argument(
        "--civici-zone",
        default=config.CIVICI_PRG_REASSIGNED_CSV,
    )
    parser.add_argument(
        "--out",
        default=config.ANNUAL_COMPLETE_PARQUET,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    annuo = f"read_csv_auto({sql_path(args.annuo)}, sample_size = -1)"
    civici_zone = f"read_csv_auto({sql_path(args.civici_zone)}, all_varchar = true)"

    duplicate = connection.execute(
        f"""
        SELECT PROGRESSIVO_ACCESSO, count(*) AS n
        FROM {civici_zone}
        GROUP BY PROGRESSIVO_ACCESSO
        HAVING count(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if duplicate:
        raise SystemExit(
            f"Classificazione PRG duplicata per id_civico={duplicate[0]!r}."
        )

    query = f"""
            SELECT
                civ_ann.*,
                try_cast(civ_zona.COORD_X_COMUNE AS DOUBLE) AS coord_x_prg,
                try_cast(civ_zona.COORD_Y_COMUNE AS DOUBLE) AS coord_y_prg,
                civ_zona.prg_codice_zona AS prg_codice_zona_originale,
                civ_zona.prg_zona AS prg_zona_originale,
                civ_zona.prg_norme AS prg_norme_originali,
                civ_zona.prg_normative AS prg_normative_originali,
                civ_zona.prg_fonte AS prg_fonte_originale,
                civ_zona.prg_gid AS prg_gid_originale,
                civ_zona.prg_id AS prg_id_originale,
                civ_zona.prg_feature_id AS prg_feature_id_originale,
                civ_zona.prg_error,
                civ_zona.prg_codice_zona_new AS zona_costruzione,
                civ_zona.prg_zona_new AS prg_zona,
                civ_zona.prg_norme_new AS prg_norme,
                civ_zona.prg_normative_new AS prg_normative,
                civ_zona.prg_fonte_new AS prg_fonte,
                civ_zona.prg_gid_new AS prg_gid,
                civ_zona.prg_id_new AS prg_id,
                civ_zona.prg_feature_id_new AS prg_feature_id,
                civ_zona.reassignment_distance IS NOT NULL
                    AS zona_costruzione_riassegnata,
                try_cast(civ_zona.reassignment_distance AS DOUBLE)
                    AS distanza_riassegnazione_prg
            FROM {annuo} AS civ_ann
            LEFT JOIN {civici_zone} AS civ_zona
                ON civ_ann.id_civico = civ_zona.PROGRESSIVO_ACCESSO
    """

    input_rows = connection.execute(f"SELECT count(*) FROM {annuo}").fetchone()[0]
    output_rows = connection.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
    if output_rows != input_rows:
        raise SystemExit(
            f"Il join PRG ha modificato il numero di righe: {input_rows} -> {output_rows}."
        )

    connection.execute(
        f"""
        COPY (
            {query}
        ) TO {sql_path(args.out)} ({copy_options(args.out)})
        """
    )


if __name__ == "__main__":
    main()
