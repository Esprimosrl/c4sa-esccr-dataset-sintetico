#!/usr/bin/env python3

from pathlib import Path
import argparse
import duckdb


def sql_lit(value: str) -> str:
    """Quote a string literal for SQL."""
    return "'" + value.replace("'", "''") + "'"


def sql_ident(value: str) -> str:
    """Quote a SQL identifier, e.g. column name."""
    return '"' + value.replace('"', '""') + '"'


def detect_input_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".parquet", ".pq"}:
        return "parquet"
    raise ValueError(
        "Could not detect input format. Use --input-format csv or parquet."
    )


def export_options(output_format: str) -> str:
    if output_format == "parquet":
        return "(FORMAT parquet)"
    if output_format == "csv":
        return "(FORMAT csv, HEADER)"
    raise ValueError("Unsupported output format.")


def output_path(output_dir: Path, name: str, output_format: str) -> str:
    ext = "parquet" if output_format == "parquet" else "csv"
    return str(output_dir / f"{name}.{ext}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a sampled dataset with N samples per id_civico per year."
    )

    parser.add_argument("--input", required=True, help="Input CSV or Parquet file.")
    parser.add_argument(
        "--input-format", choices=["auto", "csv", "parquet"], default="auto"
    )
    parser.add_argument("--output-dir", default="output_sampled")

    parser.add_argument("--samples-per-year", "-N", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--id-col", default="id_civico")
    parser.add_argument("--date-col", default="data")
    parser.add_argument("--year-col", default="anno_produzione")
    parser.add_argument("--target-col", default="conferimento_kg")

    parser.add_argument("--date-format", default="%d/%m/%Y")
    parser.add_argument(
        "--output-format", choices=["parquet", "csv"], default="parquet"
    )

    parser.add_argument(
        "--write-holes",
        action="store_true",
        help="Also export the rows that were not sampled.",
    )

    args = parser.parse_args()

    if args.samples_per_year <= 0:
        raise ValueError("--samples-per-year must be greater than 0.")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    input_format = args.input_format
    if input_format == "auto":
        input_format = detect_input_format(input_path)

    if input_format == "csv":
        source_sql = f"""
            read_csv_auto(
                {sql_lit(str(input_path))},
                header = true,
                all_varchar = true
            )
        """
    else:
        source_sql = f"read_parquet({sql_lit(str(input_path))})"

    id_col = sql_ident(args.id_col)
    date_col = sql_ident(args.date_col)
    year_col = sql_ident(args.year_col)
    target_col = sql_ident(args.target_col)

    opts = export_options(args.output_format)

    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE raw_data AS
        SELECT
            row_number() OVER () AS __row_id,
            *
        FROM {source_sql};
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE normalized AS
        WITH typed AS (
            SELECT
                *,
                COALESCE(
                    TRY_CAST({date_col} AS DATE),
                    TRY_STRPTIME(
                        CAST({date_col} AS VARCHAR),
                        {sql_lit(args.date_format)}
                    )::DATE
                ) AS data_dt,

                TRY_CAST({year_col} AS INTEGER) AS anno_from_col,

                TRY_CAST(
                    REPLACE(CAST({target_col} AS VARCHAR), ',', '.')
                    AS DOUBLE
                ) AS target_value
            FROM raw_data
        )
        SELECT
            *,
            COALESCE(
                anno_from_col,
                EXTRACT(YEAR FROM data_dt)::INTEGER
            ) AS anno_ref
        FROM typed
        WHERE {id_col} IS NOT NULL
          AND data_dt IS NOT NULL
          AND target_value IS NOT NULL;
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sampled AS
        WITH ordered_days AS (
            SELECT
                *,
                ntile({args.samples_per_year}) OVER (
                    PARTITION BY {id_col}, anno_ref
                    ORDER BY data_dt
                ) AS year_bin,

                count(*) OVER (
                    PARTITION BY {id_col}, anno_ref
                ) AS group_rows
            FROM normalized
        ),

        ranked_in_bin AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY {id_col}, anno_ref, year_bin
                    ORDER BY hash(
                        CAST(__row_id AS VARCHAR)
                        || '|sample|'
                        || CAST({args.seed} AS VARCHAR)
                    )
                ) AS bin_rank
            FROM ordered_days
        )

        SELECT
            *,
            year_bin AS sample_rank,
            {args.samples_per_year} AS samples_requested
        FROM ranked_in_bin
        WHERE bin_rank = 1;
    """)

    con.execute(f"""
        COPY sampled
        TO {sql_lit(output_path(output_dir, "sampled_all", args.output_format))}
        {opts};
    """)

    if args.write_holes:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE holes AS
            SELECT n.*
            FROM normalized n
            ANTI JOIN sampled s
            ON n.__row_id = s.__row_id;
        """)

        con.execute(f"""
            COPY holes
            TO {sql_lit(output_path(output_dir, "holes_not_sampled", args.output_format))}
            {opts};
        """)

    summary = con.execute(f"""
        SELECT
            anno_ref,
            count(DISTINCT {id_col}) AS id_civico_count,
            count(*) AS sampled_rows
        FROM sampled
        GROUP BY anno_ref
        ORDER BY anno_ref;
    """).fetchall()

    group_summary = con.execute("""
        SELECT
            min(sample_rank) AS min_sample_rank,
            max(sample_rank) AS max_sample_rank,
            min(group_rows) AS min_original_group_rows,
            max(group_rows) AS max_original_group_rows,
            count(*) AS sampled_rows
        FROM sampled;
    """).fetchone()

    print("Created files in:", output_dir)

    print("\nYear summary:")
    for anno_ref, id_count, sampled_rows in summary:
        print(f"  {anno_ref}: {sampled_rows} sampled rows across {id_count} id_civico")

    print("\nSampling summary:")
    print(f"  requested samples per id_civico/year: {args.samples_per_year}")
    print(f"  sampled rows: {group_summary[4]}")
    print(f"  max sample rank used: {group_summary[1]}")
    print(f"  min original group size: {group_summary[2]}")
    print(f"  max original group size: {group_summary[3]}")


if __name__ == "__main__":
    main()
