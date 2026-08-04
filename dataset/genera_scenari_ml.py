#!/usr/bin/env python3

import argparse
import hashlib
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset.distribuzione_giornaliera import (
    FRACTIONS,
    build_collection_map,
    get_annual_columns,
    get_zones_from_annual,
    read_calendar_events,
    resolve_annual_col,
)


GENERATOR = ROOT / "dataset" / "distribuzione_giornaliera.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Genera scenari giornalieri con seed diversi, li aggrega per zona e "
            "conserva soltanto i Parquet e i manifest necessari al machine learning."
        )
    )
    parser.add_argument(
        "--annuo",
        default="data_csv_parquet/andria_distribuzione_annuale_completa.parquet",
        help="Parquet annuale completo, mantenuto fisso per tutti gli scenari.",
    )
    parser.add_argument(
        "--calendario",
        default="data_csv_parquet/calendario_raccolta.csv",
        help="Calendario usato dalla distribuzione giornaliera.",
    )
    parser.add_argument(
        "--out-dir",
        default="data_csv_parquet/scenari_ml",
        help="Directory contenente una partizione per scenario_seed.",
    )
    parser.add_argument("--anno", type=int, default=2024)
    parser.add_argument("--seed-annuale", type=int, default=42)
    parser.add_argument("--seed-inizio", type=int, default=42)
    parser.add_argument("--seed-fine", type=int, default=61)
    parser.add_argument("--spillover-days", type=int, default=14)
    parser.add_argument(
        "--missing-calendar-mode",
        choices=["error", "daily"],
        default="daily",
    )
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rigenera gli scenari già completi.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra i comandi senza generare file.",
    )
    return parser.parse_args()


def resolve_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_spine(
    calendar_events: list[dict],
    zones: list[str],
    year: int,
    spillover_days: int,
    missing_calendar_mode: str,
) -> pl.DataFrame:
    frames = []
    for fraction in FRACTIONS:
        collection_map = build_collection_map(
            calendar_events=calendar_events,
            zones=zones,
            fraction=fraction,
            year=year,
            spillover_days=spillover_days,
            missing_calendar_mode=missing_calendar_mode,
            force_year_end=True,
        )
        frames.append(
            collection_map.select(
                [
                    "zona_raccolta",
                    "frazione",
                    pl.col("conferimento_data").alias("data"),
                ]
            ).unique()
        )

    return pl.concat(frames).unique().sort(["data", "zona_raccolta", "frazione"])


def annual_totals(path: Path) -> dict[str, float]:
    columns = get_annual_columns(str(path), ",")
    expressions = []
    labels = []

    for fraction in FRACTIONS:
        column = resolve_annual_col(columns, fraction)
        if column is None:
            raise ValueError(f"Colonna annuale mancante: {fraction.annual_col}")
        labels.append(fraction.label)
        expressions.append(
            pl.col(column).cast(pl.Float64).sum().alias(fraction.label)
        )

    row = pl.scan_parquet(path).select(expressions).collect().row(0)
    return dict(zip(labels, (float(value or 0.0) for value in row), strict=True))


def aggregate_scenario(
    raw_path: Path,
    out_path: Path,
    annual_path: Path,
    calendar_events: list[dict],
    zones: list[str],
    year: int,
    seed: int,
    spillover_days: int,
    missing_calendar_mode: str,
) -> dict:
    targets = (
        pl.scan_parquet(raw_path)
        .select(
            [
                pl.col("data").cast(pl.Utf8),
                pl.col("zona_raccolta").cast(pl.Utf8),
                pl.col("frazione").cast(pl.Utf8),
                pl.col("conferimento_t").cast(pl.Float64),
            ]
        )
        .group_by(["data", "zona_raccolta", "frazione"])
        .agg(pl.sum("conferimento_t"))
    )

    spine = build_spine(
        calendar_events,
        zones,
        year,
        spillover_days,
        missing_calendar_mode,
    )
    result = (
        spine.lazy()
        .join(
            targets,
            on=["data", "zona_raccolta", "frazione"],
            how="left",
        )
        .with_columns(
            [
                pl.lit(seed).cast(pl.Int32).alias("scenario_seed"),
                pl.lit(year).cast(pl.Int32).alias("anno_produzione"),
                pl.col("conferimento_t").fill_null(0.0),
            ]
        )
        .select(
            [
                "scenario_seed",
                "anno_produzione",
                "data",
                "zona_raccolta",
                "frazione",
                "conferimento_t",
            ]
        )
        .sort(["data", "zona_raccolta", "frazione"])
        .collect(engine="streaming")
    )

    keys = ["scenario_seed", "data", "zona_raccolta", "frazione"]
    if result.select(pl.struct(keys).n_unique()).item() != result.height:
        raise ValueError(f"Chiave scenario duplicata per seed {seed}")
    if result["conferimento_t"].min() < 0:
        raise ValueError(f"Target negativo per seed {seed}")

    expected = annual_totals(annual_path)
    observed = dict(
        result.group_by("frazione")
        .agg(pl.sum("conferimento_t"))
        .iter_rows()
    )
    for fraction, total in expected.items():
        actual = float(observed.get(fraction, 0.0))
        if not math.isclose(actual, total, rel_tol=1e-10, abs_tol=1e-9):
            raise ValueError(
                f"Riconciliazione fallita per seed={seed}, frazione={fraction}: "
                f"{actual:.12f} != {total:.12f}"
            )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_parquet(out_path, compression="zstd")

    return {
        "rows": result.height,
        "zones": sorted(result["zona_raccolta"].unique().to_list()),
        "fractions": sorted(result["frazione"].unique().to_list()),
        "date_min": result["data"].min(),
        "date_max": result["data"].max(),
    }


def generator_command(
    args: argparse.Namespace,
    annual_path: Path,
    calendar_path: Path,
    raw_path: Path,
    seed: int,
) -> list[str]:
    return [
        args.python,
        str(GENERATOR),
        "--annuo",
        str(annual_path),
        "--anno",
        str(args.anno),
        "--calendario",
        str(calendar_path),
        "--id-col",
        "id_civico",
        "--zona-col",
        "zona_raccolta",
        "--seed",
        str(seed),
        "--out",
        str(raw_path),
        "--spillover-days",
        str(args.spillover_days),
        "--missing-calendar-mode",
        args.missing_calendar_mode,
    ]


def main() -> None:
    args = parse_args()
    if args.seed_fine < args.seed_inizio:
        raise SystemExit("--seed-fine deve essere maggiore o uguale a --seed-inizio")
    if args.spillover_days < 0:
        raise SystemExit("--spillover-days non può essere negativo")

    annual_path = resolve_path(args.annuo)
    calendar_path = resolve_path(args.calendario)
    out_dir = resolve_path(args.out_dir)

    for path in (annual_path, calendar_path):
        if not path.exists():
            raise SystemExit(f"Input mancante: {path}")
    if annual_path.suffix.lower() != ".parquet":
        raise SystemExit("--annuo deve essere il Parquet annuale autorevole")

    annual_columns = get_annual_columns(str(annual_path), ",")
    zones = get_zones_from_annual(
        str(annual_path),
        ",",
        annual_columns,
        "zona_raccolta",
        "1",
    )
    calendar_events = read_calendar_events(
        str(calendar_path),
        ",",
        zones,
        args.anno,
        args.spillover_days,
    )
    generator_hash = file_sha256(GENERATOR)
    builder_hash = file_sha256(Path(__file__))
    annual_hash = file_sha256(annual_path)
    calendar_hash = file_sha256(calendar_path)

    for seed in range(args.seed_inizio, args.seed_fine + 1):
        scenario_dir = out_dir / f"scenario_seed={seed}"
        target_path = scenario_dir / "ritiri_zona.parquet"
        manifest_path = scenario_dir / "manifest.json"
        raw_path = scenario_dir / ".conferimenti_civici.parquet"
        command = generator_command(
            args,
            annual_path,
            calendar_path,
            raw_path,
            seed,
        )

        if args.dry_run:
            print(" ".join(command))
            continue

        complete = target_path.exists() and manifest_path.exists()
        partial = target_path.exists() != manifest_path.exists()
        if complete and not args.force:
            print(f"[seed {seed}] cached")
            continue
        if partial and not args.force:
            raise SystemExit(
                f"Scenario incompleto per seed {seed}; usa --force per rigenerarlo."
            )

        scenario_dir.mkdir(parents=True, exist_ok=True)
        if args.force and raw_path.exists():
            raw_path.unlink()

        print(f"[seed {seed}] generazione giornaliera", flush=True)
        subprocess.run(command, cwd=ROOT, check=True)

        print(f"[seed {seed}] aggregazione per zona", flush=True)
        stats = aggregate_scenario(
            raw_path=raw_path,
            out_path=target_path,
            annual_path=annual_path,
            calendar_events=calendar_events,
            zones=zones,
            year=args.anno,
            seed=seed,
            spillover_days=args.spillover_days,
            missing_calendar_mode=args.missing_calendar_mode,
        )

        manifest = {
            "anno": args.anno,
            "annual_seed": args.seed_annuale,
            "scenario_seed": seed,
            "annual_dataset": str(annual_path),
            "annual_dataset_sha256": annual_hash,
            "calendar": str(calendar_path),
            "calendar_sha256": calendar_hash,
            "dataset": str(target_path),
            "dataset_sha256": file_sha256(target_path),
            "spillover_days": args.spillover_days,
            "missing_calendar_mode": args.missing_calendar_mode,
            "generator": str(GENERATOR),
            "generator_sha256": generator_hash,
            "scenario_builder_sha256": builder_hash,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            **stats,
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        raw_path.unlink()
        print(f"[seed {seed}] creato {target_path}", flush=True)


if __name__ == "__main__":
    main()
