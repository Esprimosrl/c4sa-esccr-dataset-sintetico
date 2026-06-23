#!/usr/bin/env python3

import argparse
from pathlib import Path
from zipfile import ZipFile
import csv
import shutil

import pandas as pd
import requests


ZIP_URL = "https://anncsu.open.agenziaentrate.gov.it/age-inspire/opendata/anncsu/getds.php?STRAD_PUGL"
ANDRIA_CODICE_ISTAT = "110001"

DEFAULT_WORK_DIR = Path("anncsu_stradario_puglia_andria")


def download_zip(url: str, destination: Path) -> None:
    headers = {"User-Agent": "Mozilla/5.0"}

    with requests.get(url, headers=headers, stream=True, timeout=60) as response:
        response.raise_for_status()

        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)


def extract_only_csv(zip_path: Path, extract_dir: Path) -> Path:
    if extract_dir.exists():
        shutil.rmtree(extract_dir)

    extract_dir.mkdir(parents=True, exist_ok=True)

    with ZipFile(zip_path, "r") as zip_file:
        csv_files = [
            name
            for name in zip_file.namelist()
            if name.lower().endswith(".csv") and not name.endswith("/")
        ]

        if len(csv_files) != 1:
            raise RuntimeError(
                f"Expected exactly one CSV in the ZIP, found {len(csv_files)}: {csv_files}"
            )

        zip_file.extract(csv_files[0], extract_dir)

    return extract_dir / csv_files[0]


def detect_delimiter(csv_path: Path, encoding: str = "utf-8") -> str:
    with csv_path.open("r", encoding=encoding, errors="replace", newline="") as file:
        sample = file.read(8192)

    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(sample, delimiters=";,|\t,")
    return dialect.delimiter


def find_codice_istat_column(columns: list[str]) -> str:
    """
    Finds the likely ISTAT code column without assuming the exact ANNCSU header name.
    Adjust this if your file uses a known fixed header.
    """
    normalized = {
        col: (
            col.lower()
            .replace(" ", "")
            .replace("_", "")
            .replace("-", "")
            .replace(".", "")
        )
        for col in columns
    }

    preferred_matches = [
        "codiceistat",
        "codistat",
        "codicecomuneistat",
        "codicecomune",
        "istat",
        "procom",
    ]

    for target in preferred_matches:
        for original, norm in normalized.items():
            if norm == target:
                return original

    for original, norm in normalized.items():
        if "istat" in norm:
            return original

    raise RuntimeError(
        f"Could not find the ISTAT code column. Available columns are: {columns}"
    )


def filter_andria_rows(source_csv: Path, output_csv: Path) -> int:
    delimiter = detect_delimiter(source_csv)

    # ANNCSU files are usually UTF-8, but this keeps the script tolerant.
    df = pd.read_csv(
        source_csv,
        sep=delimiter,
        dtype=str,
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )

    df.columns = [str(col).strip() for col in df.columns]

    istat_column = find_codice_istat_column(list(df.columns))

    # Keep leading zeroes if present and normalize whitespace.
    mask = df[istat_column].astype(str).str.strip() == ANDRIA_CODICE_ISTAT
    andria_df = df.loc[mask].copy()

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    andria_df.to_csv(output_csv, index=False, encoding="utf-8-sig", sep=";")

    print(f"Source CSV: {source_csv}")
    print(f"Detected delimiter: {repr(delimiter)}")
    print(f"Detected ISTAT column: {istat_column}")
    print(f"Andria rows written: {len(andria_df)}")
    print(f"Output CSV: {output_csv}")

    return len(andria_df)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scarica i civici ANNCSU di Andria.")
    parser.add_argument("--out", default="data_csv_parquet/civici_andria.csv")
    parser.add_argument("--work-dir", default=str(DEFAULT_WORK_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.work_dir)
    zip_path = output_dir / "stradario_puglia.zip"
    extract_dir = output_dir / "extracted"
    output_csv = Path(args.out)

    output_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading ZIP...")
    download_zip(ZIP_URL, zip_path)

    print("Extracting CSV...")
    source_csv = extract_only_csv(zip_path, extract_dir)

    print("Filtering Andria rows...")
    rows = filter_andria_rows(source_csv, output_csv)

    if rows == 0:
        raise RuntimeError(
            f"No rows found for Codice ISTAT {ANDRIA_CODICE_ISTAT}. "
            "Check the ISTAT-code column name and value format in the source CSV."
        )


if __name__ == "__main__":
    main()
