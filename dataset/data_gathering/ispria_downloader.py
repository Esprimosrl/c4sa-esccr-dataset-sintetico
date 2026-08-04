#!/usr/bin/env python3

import argparse
import csv
import re
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dataset import config


URL_TEMPLATE = (
    "https://www.catasto-rifiuti.isprambiente.it/index.php"
    "?pg=mDetComune"
    "&aa={anno}"
    "&regidb=16"
    "&nomereg=Puglia"
    "&providb=110"
    "&nomeprov=Barletta-Andria-Trani"
    "&regid=16110001"
    "&nomecom=ANDRIA"
    "&cerca=cerca"
    "&&p=1"
)

DEFAULT_OUTPUT_DIR = Path(config.ISPRA_WORK_DIR)

FRACTIONS = [
    ("Altro RD", ["altro rd"]),
    ("Ingombranti misti", ["ingombranti misti"]),
    ("Carta e cartone", ["carta e cartone", "carta cartone"]),
    ("Frazione Organica", ["frazione organica", "organico"]),
    ("Legno", ["legno"]),
    ("Metallo", ["metallo", "metalli"]),
    ("Plastica", ["plastica"]),
    ("RAEE", ["raee"]),
    ("Selettiva", ["selettiva"]),
    ("Tessili", ["tessili"]),
    ("Vetro", ["vetro"]),
    ("Rifiuti da C&D", ["rifiuti da c&d", "c&d", "rifiuti da c e d"]),
    ("Pulizia stradale a recupero", ["pulizia stradale a recupero"]),
]


def clean_filename(value: str) -> str:
    value = re.sub(r"[^\w\s.-]", "", value, flags=re.UNICODE)
    value = re.sub(r"\s+", "_", value.strip())
    return value[:80] or "table"


def normalize(value: object) -> str:
    value = str(value).lower()
    for old, new in {
        "à": "a",
        "è": "e",
        "é": "e",
        "ì": "i",
        "ò": "o",
        "ù": "u",
    }.items():
        value = value.replace(old, new)
    return re.sub(r"\s+", " ", value).strip()


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0 Safari/537.36"
        )
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    # The ISPRA page is Italian; this avoids mojibake in accented text.
    response.encoding = response.apparent_encoding

    return response.text


def extract_tables(html: str) -> list[pd.DataFrame]:
    # pandas.read_html is usually the simplest and most reliable way
    # to extract real <table> elements from an HTML page.
    try:
        tables = pd.read_html(html, flavor="bs4")
    except ValueError:
        tables = []
    except ImportError:
        tables = extract_tables_with_bs4(html)

    return tables


def extract_tables_with_bs4(html: str) -> list[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    tables = []

    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)

        if rows:
            tables.append(pd.DataFrame(rows))

    return tables


def parse_number(value: object) -> float | None:
    s = str(value).strip()
    if not re.search(r"\d+[,.]\d+", s):
        return None

    match = re.search(r"\d[\d.,]*", s)
    if not match:
        return None

    s = match.group(0)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def find_fraction_value(tables: list[pd.DataFrame], aliases: list[str]) -> float:
    normalized_aliases = [normalize(alias) for alias in aliases]

    for df in tables:
        for _, row in df.dropna(how="all").iterrows():
            cells = list(row)
            text = normalize(" ".join(str(cell) for cell in cells))
            if not any(alias in text for alias in normalized_aliases):
                continue

            numbers = [parse_number(cell) for cell in cells]
            numbers = [number for number in numbers if number is not None]
            if numbers:
                return max(numbers)

    return 0.0


def extract_annual_totals(html: str, anno: int) -> dict[str, float]:
    match = re.search(
        rf"\['{anno}'\s*,\s*'Comune di Andria'\s*,(?P<body>.*?)\],",
        html,
        flags=re.DOTALL,
    )
    if not match:
        return {}

    values = [
        float(value)
        for value in re.findall(r"\{v:\s*([-+]?\d+(?:\.\d+)?)\s*,", match.group("body"))
    ]
    if len(values) < len(FRACTIONS):
        return {}

    return {
        label: values[index]
        for index, (label, _) in enumerate(FRACTIONS)
    }


def extract_table_totals(tables: list[pd.DataFrame]) -> dict[str, float]:
    return {
        label: find_fraction_value(tables, aliases)
        for label, aliases in FRACTIONS
    }


def write_totals(values: dict[str, float], anno: int, out: Path) -> None:
    row = {
        "Anno": anno,
        "Dato relativo a:": "Comune di Andria",
    }
    for label, _ in FRACTIONS:
        row[label] = f"{values.get(label, 0.0):.3f}"

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def table_title_nearby(html: str, table_index: int) -> str:
    """
    Attempts to name the table using the nearest previous heading.
    Falls back to table_01, table_02, etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")

    if table_index >= len(tables):
        return f"table_{table_index + 1:02d}"

    table = tables[table_index]

    for previous in table.find_all_previous(["h1", "h2", "h3", "h4", "caption"]):
        text = previous.get_text(" ", strip=True)
        if text:
            return clean_filename(text)

    return f"table_{table_index + 1:02d}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scarica i totali rifiuti ISPRA di Andria.")
    parser.add_argument("--anno", type=int, default=config.DEFAULT_ANNO)
    parser.add_argument("--out", default=config.TOTALI_CSV)
    parser.add_argument("--work-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.work_dir)
    output_dir.mkdir(exist_ok=True)

    html = fetch_html(URL_TEMPLATE.format(anno=args.anno))

    # Save raw HTML for debugging/reproducibility.
    html_path = output_dir / "source_page.html"
    html_path.write_text(html, encoding="utf-8")

    values = extract_annual_totals(html, args.anno)
    tables = extract_tables(html)

    if not values and not tables:
        print("No HTML tables were found on the page.")
        print(f"Saved the page source here: {html_path}")
        print(
            "The data may be rendered as charts or loaded dynamically. "
            "Inspect source_page.html or use browser automation if needed."
        )
        return

    if not values:
        values = extract_table_totals(tables)

    write_totals(values, args.anno, Path(args.out))

    excel_path = output_dir / f"ispra_andria_{args.anno}_tables.xlsx"

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for i, df in enumerate(tables):
            title = table_title_nearby(html, i)
            csv_path = output_dir / f"{i + 1:02d}_{title}.csv"

            # Basic cleanup.
            df = df.dropna(how="all")
            df = df.dropna(axis=1, how="all")

            df.to_csv(csv_path, index=False, encoding="utf-8-sig")

            sheet_name = clean_filename(title)[:31] or f"Table_{i + 1}"
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"Saved table {i + 1}: {csv_path}")

    print(f"Saved totals CSV: {args.out}")
    print(f"Saved Excel workbook: {excel_path}")


if __name__ == "__main__":
    main()
