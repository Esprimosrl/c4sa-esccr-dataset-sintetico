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
    parser = argparse.ArgumentParser(description='Riassegna i civici classificati come "viab" alla zona vicina.')
    parser.add_argument("--civici", default=config.CIVICI_PRG_CSV)
    parser.add_argument("--zone", default=config.FULL_WMS_ZONE_GEOJSON)
    parser.add_argument("--out", default=config.CIVICI_PRG_REASSIGNED_CSV)
    parser.add_argument("--cell-deg", type=float, default=config.DEFAULT_REASSIGN_CELL_DEGREES)
    parser.add_argument("--zones-per-chunk", type=int, default=config.DEFAULT_REASSIGN_ZONES_PER_CHUNK)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
    except duckdb.Error:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")

    con.execute(
        f"""
        COPY (
            WITH
            params AS (
                SELECT
                    {args.cell_deg}::DOUBLE AS cell_deg,
                    {args.zones_per_chunk}::INT AS zones_per_chunk
            ),
            civici AS (
                SELECT
                    row_number() OVER () AS row_id,
                    *,
                    replace(COORD_X_COMUNE, ',', '.')::DOUBLE AS x,
                    replace(COORD_Y_COMUNE, ',', '.')::DOUBLE AS y,
                    ST_Point(
                        replace(COORD_X_COMUNE, ',', '.')::DOUBLE,
                        replace(COORD_Y_COMUNE, ',', '.')::DOUBLE
                    ) AS pt
                FROM read_csv_auto({sql_path(args.civici)}, header = true)
            ),
            zone AS (
                SELECT
                    id::VARCHAR AS zone_feature_id,
                    "Codice zona" AS codice_zona,
                    Zona AS zona,
                    Norme AS norme,
                    normative,
                    Fonte AS fonte,
                    gid,
                    id,
                    geom
                FROM ST_Read({sql_path(args.zone)})
                WHERE lower("Codice zona") <> 'viab'
            ),
            viab AS (
                SELECT
                    c.*,
                    floor(c.x / p.cell_deg)::INT || ':' || floor(c.y / p.cell_deg)::INT AS chunk_id
                FROM civici c, params p
                WHERE lower(c.prg_codice_zona) = 'viab'
            ),
            chunk_centers AS (
                SELECT
                    chunk_id,
                    ST_Point(avg(x), avg(y)) AS center_pt
                FROM viab
                GROUP BY chunk_id
            ),
            chunk_nearest_codes AS (
                SELECT *
                FROM (
                    SELECT
                        cc.chunk_id,
                        z.*,
                        ST_Distance(cc.center_pt, z.geom) AS dist,
                        row_number() OVER (
                            PARTITION BY cc.chunk_id, z.codice_zona
                            ORDER BY ST_Distance(cc.center_pt, z.geom)
                        ) AS same_code_rank
                    FROM chunk_centers cc
                    JOIN zone z
                        ON ST_DWithin(cc.center_pt, z.geom, 0.02)
                )
                WHERE same_code_rank = 1
            ),
            chunk_choices AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY chunk_id
                            ORDER BY dist
                        ) AS choice_rank
                    FROM chunk_nearest_codes
                ), params p
                WHERE choice_rank <= p.zones_per_chunk
            ),
            viab_ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY chunk_id
                        ORDER BY row_id
                    ) AS seq_in_chunk
                FROM viab
            ),
            reassignment AS (
                SELECT
                    v.row_id,
                    z.*
                FROM viab_ranked v
                JOIN chunk_choices z
                    ON z.chunk_id = v.chunk_id
                   AND z.choice_rank = 1 + ((v.seq_in_chunk - 1) % (
                       SELECT count(*)
                       FROM chunk_choices z2
                       WHERE z2.chunk_id = v.chunk_id
                   ))
            ),
            moved AS (
                SELECT
                    c.row_id,
                    r.*,
                    ST_ClosestPoint(ST_Boundary(r.geom), c.pt) AS moved_pt
                FROM civici c
                JOIN reassignment r USING (row_id)
            )
            SELECT
                c.* EXCLUDE (row_id, x, y, pt, COORD_X_COMUNE, COORD_Y_COMUNE),
                CASE
                    WHEN m.row_id IS NOT NULL THEN ST_X(m.moved_pt)
                    ELSE c.x
                END AS COORD_X_COMUNE,
                CASE
                    WHEN m.row_id IS NOT NULL THEN ST_Y(m.moved_pt)
                    ELSE c.y
                END AS COORD_Y_COMUNE,
                CASE WHEN m.row_id IS NOT NULL THEN m.codice_zona ELSE c.prg_codice_zona END AS prg_codice_zona_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.zona        ELSE c.prg_zona        END AS prg_zona_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.norme       ELSE c.prg_norme       END AS prg_norme_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.normative   ELSE c.prg_normative   END AS prg_normative_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.fonte       ELSE c.prg_fonte       END AS prg_fonte_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.gid         ELSE c.prg_gid         END AS prg_gid_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.id          ELSE c.prg_id          END AS prg_id_new,
                CASE WHEN m.row_id IS NOT NULL THEN m.zone_feature_id ELSE c.prg_feature_id END AS prg_feature_id_new,
                m.dist AS reassignment_distance
            FROM civici c
            LEFT JOIN moved m USING (row_id)
        ) TO {sql_path(args.out)} (HEADER, DELIMITER ',')
        """
    )


if __name__ == "__main__":
    main()
