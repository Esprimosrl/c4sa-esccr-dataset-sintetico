#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from joblib import Parallel, delayed


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Task:
    name: str
    cmd: tuple[str, ...]
    deps: tuple[str, ...] = ()
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Config:
    python: str
    anno: int
    seed: int


def tasks(config: Config) -> list[Task]:
    py = config.python
    anno = str(config.anno)
    seed = str(config.seed)

    return [
        Task(
            "civici",
            (
                py,
                "dataset/data_gathering/anncsu_downloader.py",
                "--out",
                "data_csv_parquet/civici_andria.csv",
            ),
            outputs=("data_csv_parquet/civici_andria.csv",),
        ),
        Task(
            "totali",
            (
                py,
                "dataset/data_gathering/ispria_downloader.py",
                "--anno",
                anno,
                "--out",
                "data_csv_parquet/totali.csv",
            ),
            outputs=("data_csv_parquet/totali.csv",),
        ),
        Task(
            "distribuzione_annuale",
            (
                py,
                "dataset/distribuzione_annuale.py",
                "--civici",
                "data_csv_parquet/civici_andria.csv",
                "--totali",
                "data_csv_parquet/totali.csv",
                "--out",
                "data_csv_parquet/andria_distribuzione_annuale.csv",
                "--seed",
                seed,
                "--totali-separator",
                ",",
            ),
            deps=("civici", "totali"),
            inputs=(
                "data_csv_parquet/civici_andria.csv",
                "data_csv_parquet/totali.csv",
            ),
            outputs=("data_csv_parquet/andria_distribuzione_annuale.csv",),
        ),
        Task(
            "merge_zone",
            (py, "dataset/query_merge_zone.py"),
            deps=("distribuzione_annuale",),
            inputs=(
                "data_csv_parquet/andria_distribuzione_annuale.csv",
                "data_csv_parquet/andria_stradario_zone.csv",
            ),
            outputs=("data_csv_parquet/andria_distribuzione_annuale_zone.csv",),
        ),
        Task(
            "inferisci_zone",
            (py, "dataset/query_inferisci_zone_mancanti.py"),
            deps=("merge_zone",),
            inputs=("data_csv_parquet/andria_distribuzione_annuale_zone.csv",),
            outputs=(
                "data_csv_parquet/andria_distribuzione_annuale_zone_inferite.csv",
            ),
        ),
        Task(
            "full_wms_zone",
            (
                py,
                "dataset/data_gathering/full_wms_zone_dump.py",
                "--out",
                "data_csv_parquet/andria_full_zone.geojson",
            ),
            outputs=("data_csv_parquet/andria_full_zone.geojson",),
        ),
        Task(
            "classifica_civici_prg",
            (
                py,
                "dataset/data_gathering/scrape_wms.py",
                "--x-field",
                "COORD_X_COMUNE",
                "--y-field",
                "COORD_Y_COMUNE",
                "--sleep",
                "5.3",
                "--polygons-out",
                "data_csv_parquet/poligoni_wms.geojson",
                "data_csv_parquet/civici_andria.csv",
                "--polygons-source",
                "data_csv_parquet/andria_full_zone.geojson",
                "data_csv_parquet/civici_andria_classificati_per_zone_di_costruzione.csv",
            ),
            deps=("civici", "full_wms_zone"),
            inputs=(
                "data_csv_parquet/civici_andria.csv",
                "data_csv_parquet/andria_full_zone.geojson",
            ),
            outputs=(
                "data_csv_parquet/civici_andria_classificati_per_zone_di_costruzione.csv",
            ),
        ),
        Task(
            "togli_civici_dalla_strada",
            (py, "dataset/query_togli_civici_dalla_strada.py"),
            deps=("classifica_civici_prg", "full_wms_zone"),
            inputs=(
                "data_csv_parquet/civici_andria_classificati_per_zone_di_costruzione.csv",
                "data_csv_parquet/andria_full_zone.geojson",
            ),
            outputs=(
                "data_csv_parquet/civici_andria_riassegnati_no_viab_chunked_moved.csv",
            ),
        ),
        Task(
            "zone_costruzione_annuale",
            (
                py,
                "dataset/query_crea_distribuzione_annuale_con_zone_di_costruzione.py",
                "--civici-zone",
                "data_csv_parquet/civici_andria_riassegnati_no_viab_chunked_moved.csv",
            ),
            deps=("inferisci_zone", "togli_civici_dalla_strada"),
            inputs=(
                "data_csv_parquet/andria_distribuzione_annuale_zone_inferite.csv",
                "data_csv_parquet/civici_andria_riassegnati_no_viab_chunked_moved.csv",
            ),
            outputs=(
                "data_csv_parquet/andria_distribuzione_annuale_zone_inferite_e_zone_costruzione.csv",
            ),
        ),
        Task(
            "distribuzione_giornaliera",
            (
                py,
                "dataset/distribuzione_giornaliera.py",
                "--annuo",
                "data_csv_parquet/andria_distribuzione_annuale_zone_inferite_e_zone_costruzione.csv",
                "--anno",
                anno,
                "--calendario",
                "data_csv_parquet/calendario_raccolta.csv",
                "--id-col",
                "id_civico",
                "--zona-col",
                "zona",
                "--seed",
                seed,
                "--out",
                "data_csv_parquet/conferimenti_giornalieri_classificati.csv",
                "--out-produzione",
                "data_csv_parquet/produzione_giornaliera_classificata.csv",
                "--missing-calendar-mode",
                "daily",
            ),
            deps=("zone_costruzione_annuale",),
            inputs=(
                "data_csv_parquet/andria_distribuzione_annuale_zone_inferite_e_zone_costruzione.csv",
                "data_csv_parquet/calendario_raccolta.csv",
            ),
            outputs=(
                "data_csv_parquet/conferimenti_giornalieri_classificati.csv",
                "data_csv_parquet/produzione_giornaliera_classificata.csv",
            ),
        ),
    ]


def levels(all_tasks: list[Task]) -> list[list[Task]]:
    pending = {task.name: task for task in all_tasks}
    done: set[str] = set()
    result: list[list[Task]] = []

    while pending:
        ready = [
            task
            for task in all_tasks
            if task.name in pending and set(task.deps) <= done
        ]
        if not ready:
            raise ValueError(f"Cyclic or missing dependencies: {sorted(pending)}")

        result.append(ready)
        for task in ready:
            del pending[task.name]
            done.add(task.name)

    return result


def check_inputs(task: Task) -> None:
    missing = [path for path in task.inputs if not (ROOT / path).exists()]
    if missing:
        raise FileNotFoundError(f"{task.name}: missing inputs: {', '.join(missing)}")


def is_cached(task: Task) -> bool:
    if not task.outputs:
        return False

    output_paths = [ROOT / path for path in task.outputs]
    if any(not path.exists() for path in output_paths):
        return False

    input_paths = [ROOT / path for path in task.inputs]
    if not input_paths:
        return True

    newest_input = max(path.stat().st_mtime for path in input_paths)
    oldest_output = min(path.stat().st_mtime for path in output_paths)
    return oldest_output >= newest_input


def run_task(task: Task, force: bool) -> None:
    check_inputs(task)
    if not force and is_cached(task):
        print(f"[{task.name}] cached", flush=True)
        return

    for output in task.outputs:
        (ROOT / output).parent.mkdir(parents=True, exist_ok=True)

    print(f"[{task.name}] {shlex.join(task.cmd)}", flush=True)
    subprocess.run(task.cmd, cwd=ROOT, check=True)


def print_plan(plan: list[list[Task]]) -> None:
    for index, level in enumerate(plan, start=1):
        print(f"{index}. " + ", ".join(task.name for task in level))


def self_test() -> None:
    names_by_level = [
        [task.name for task in level]
        for level in levels(tasks(Config("python", 2024, 42)))
    ]
    assert names_by_level[0] == ["civici", "totali", "full_wms_zone"]
    assert names_by_level[-1] == ["distribuzione_giornaliera"]
    assert "zone_costruzione_annuale" in names_by_level[-2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Esegue la creazione dataset come grafo di task joblib."
    )
    parser.add_argument("--python", default=os.environ.get("PYTHON", sys.executable))
    parser.add_argument("--anno", type=int, default=int(os.environ.get("ANNO", "2024")))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("SEED", "42")))
    parser.add_argument("--jobs", type=int, default=int(os.environ.get("JOBS", "2")))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra i livelli del grafo senza eseguire.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Esegue tutti i task ignorando gli output esistenti.",
    )
    parser.add_argument("--self-test", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.self_test:
        self_test()
        return

    plan = levels(tasks(Config(args.python, args.anno, args.seed)))
    if args.dry_run:
        print_plan(plan)
        return

    for level in plan:
        Parallel(n_jobs=args.jobs, backend="threading")(
            delayed(run_task)(task, args.force) for task in level
        )


if __name__ == "__main__":
    main()
