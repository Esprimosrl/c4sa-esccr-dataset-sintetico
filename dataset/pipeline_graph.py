#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from joblib import Parallel, delayed

try:
    from dataset import config
except ModuleNotFoundError:
    import config

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


def tasks(pipeline_config: Config) -> list[Task]:
    py = pipeline_config.python
    anno = str(pipeline_config.anno)
    seed = str(pipeline_config.seed)

    return [
        Task(
            "civici",
            (
                py,
                config.ANNCSU_DOWNLOADER,
                "--out",
                config.CIVICI_CSV,
            ),
            outputs=(config.CIVICI_CSV,),
        ),
        Task(
            "totali",
            (
                py,
                config.ISPRA_DOWNLOADER,
                "--anno",
                anno,
                "--out",
                config.TOTALI_CSV,
            ),
            outputs=(config.TOTALI_CSV,),
        ),
        Task(
            "distribuzione_annuale",
            (
                py,
                config.ANNUAL_DISTRIBUTION,
                "--civici",
                config.CIVICI_CSV,
                "--totali",
                config.TOTALI_CSV,
                "--out",
                config.ANNUAL_CSV,
                "--seed",
                seed,
                "--totali-separator",
                config.DEFAULT_TOTALS_SEPARATOR,
            ),
            deps=("civici", "totali"),
            inputs=(
                config.ANNUAL_DISTRIBUTION,
                config.CIVICI_CSV,
                config.TOTALI_CSV,
            ),
            outputs=(config.ANNUAL_CSV,),
        ),
        Task(
            "merge_zone",
            (py, config.MERGE_ZONE),
            deps=("distribuzione_annuale",),
            inputs=(
                config.MERGE_ZONE,
                config.ANNUAL_CSV,
                config.STRADARIO_ZONE_CSV,
            ),
            outputs=(config.ANNUAL_ZONE_CSV,),
        ),
        Task(
            "inferisci_zone",
            (py, config.INFER_ZONE),
            deps=("merge_zone",),
            inputs=(
                config.INFER_ZONE,
                config.ANNUAL_ZONE_CSV,
            ),
            outputs=(config.ANNUAL_ZONE_INFERRED_CSV,),
        ),
        Task(
            "full_wms_zone",
            (
                py,
                config.FULL_WMS_ZONE_DUMP,
                "--out",
                config.FULL_WMS_ZONE_GEOJSON,
            ),
            outputs=(config.FULL_WMS_ZONE_GEOJSON,),
        ),
        Task(
            "classifica_civici_prg",
            (
                py,
                config.WMS_SCRAPER,
                "--x-field",
                config.WMS_X_FIELD,
                "--y-field",
                config.WMS_Y_FIELD,
                "--sleep",
                str(config.DEFAULT_WMS_SLEEP_SECONDS),
                "--polygons-out",
                config.WMS_POLYGONS_GEOJSON,
                config.CIVICI_CSV,
                "--polygons-source",
                config.FULL_WMS_ZONE_GEOJSON,
                config.CIVICI_PRG_CSV,
            ),
            deps=("civici", "full_wms_zone"),
            inputs=(
                config.CIVICI_CSV,
                config.FULL_WMS_ZONE_GEOJSON,
            ),
            outputs=(
                config.CIVICI_PRG_CSV,
            ),
        ),
        Task(
            "togli_civici_dalla_strada",
            (py, config.REASSIGN_CIVICI),
            deps=("classifica_civici_prg", "full_wms_zone"),
            inputs=(
                config.CIVICI_PRG_CSV,
                config.FULL_WMS_ZONE_GEOJSON,
            ),
            outputs=(
                config.CIVICI_PRG_REASSIGNED_CSV,
            ),
        ),
        Task(
            "zone_costruzione_annuale",
            (
                py,
                config.ADD_BUILDING_ZONE,
                "--civici-zone",
                config.CIVICI_PRG_REASSIGNED_CSV,
                "--out",
                config.ANNUAL_COMPLETE_PARQUET,
            ),
            deps=("inferisci_zone", "togli_civici_dalla_strada"),
            inputs=(
                config.ADD_BUILDING_ZONE,
                config.ANNUAL_ZONE_INFERRED_CSV,
                config.CIVICI_PRG_REASSIGNED_CSV,
            ),
            outputs=(
                config.ANNUAL_COMPLETE_PARQUET,
            ),
        ),
        Task(
            "distribuzione_giornaliera",
            (
                py,
                config.DAILY_DISTRIBUTION,
                "--annuo",
                config.ANNUAL_COMPLETE_PARQUET,
                "--anno",
                anno,
                "--calendario",
                config.CALENDARIO_RACCOLTA_CSV,
                "--id-col",
                config.DAILY_ID_COLUMN,
                "--zona-col",
                config.DAILY_ZONE_COLUMN,
                "--seed",
                seed,
                "--out",
                config.CONFERIMENTI_GIORNALIERI_PARQUET,
                "--out-produzione",
                config.PRODUZIONE_GIORNALIERA_PARQUET,
                "--missing-calendar-mode",
                config.DEFAULT_MISSING_CALENDAR_MODE,
            ),
            deps=("zone_costruzione_annuale",),
            inputs=(
                config.DAILY_DISTRIBUTION,
                config.ANNUAL_COMPLETE_PARQUET,
                config.CALENDARIO_RACCOLTA_CSV,
            ),
            outputs=(
                config.CONFERIMENTI_GIORNALIERI_PARQUET,
                config.PRODUZIONE_GIORNALIERA_PARQUET,
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
        for level in levels(tasks(Config("python", config.DEFAULT_ANNO, config.DEFAULT_SEED)))
    ]
    assert names_by_level[0] == ["civici", "totali", "full_wms_zone"]
    assert names_by_level[-1] == ["distribuzione_giornaliera"]
    assert "zone_costruzione_annuale" in names_by_level[-2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Esegue la creazione dataset come grafo di task joblib."
    )
    parser.add_argument("--python", default=os.environ.get("PYTHON", sys.executable))
    parser.add_argument("--anno", type=int, default=int(os.environ.get("ANNO", config.DEFAULT_ANNO)))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("SEED", config.DEFAULT_SEED)))
    parser.add_argument("--jobs", type=int, default=int(os.environ.get("JOBS", config.DEFAULT_JOBS)))
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
