from pathlib import Path
import csv
import requests
from collections import Counter


OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OUTPUT_FILE = Path("puglia_waste_basket_counts.csv")

PUGLIA_AREA_ID = 3600040095

QUERY = f"""
[out:csv(
  comune,
  bin_count
; true; ",")][timeout:600];

area({PUGLIA_AREA_ID})->.puglia;

node(area.puglia)["amenity"="waste_basket"]->.bins;

foreach .bins -> .bin (
  .bin;
  is_in -> .areas;

  area.areas
    ["boundary"="administrative"]
    ["admin_level"="8"]
    ["name"]
    ->.comune;

  make stat
    comune = .comune.u(t["name"]),
    bin_count = 1;

  out;
);
"""


def fetch_overpass_csv() -> str:
    response = requests.post(
        OVERPASS_URL,
        data={"data": QUERY},
        timeout=700,
        headers={"User-Agent": "puglia-waste-basket-density-script/1.2"},
    )

    if response.status_code != 200:
        print("Query sent to Overpass:")
        print(QUERY)
        raise RuntimeError(
            f"Overpass request failed with status {response.status_code}:\n"
            f"{response.text}"
        )

    return response.text


def aggregate(csv_text: str) -> list[tuple[str, int]]:
    rows = csv.DictReader(csv_text.splitlines())
    counts = Counter()

    for row in rows:
        comune = (row.get("comune") or "").strip()
        if comune:
            counts[comune] += 1

    return sorted(counts.items(), key=lambda x: x[1], reverse=True)


def main() -> None:
    raw_csv = fetch_overpass_csv()
    counts = aggregate(raw_csv)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["comune", "bins"])
        writer.writerows(counts)

    print(f"Saved aggregated CSV to: {OUTPUT_FILE.resolve()}")
    print(f"Comuni with at least one mapped waste basket: {len(counts)}")

    if counts:
        print("\nTop 10 by mapped waste basket count:")
        for comune, bins in counts[:10]:
            print(f"{comune}: {bins}")


if __name__ == "__main__":
    main()
