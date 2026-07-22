#!/usr/bin/env python3

import argparse
import bisect
import hashlib
import os
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class Fraction:
    label: str
    annual_col: str
    kind: str
    sigma: float
    base_event_probability: float = 0.0


FRACTIONS = [
    Fraction("altro_rd", "altro_rd_t", "continuous", 0.35),
    Fraction("ingombranti_misti", "ingombranti_misti_t", "rare", 1.10, 0.010),
    Fraction("carta_cartone", "carta_cartone_t", "continuous", 0.45),
    Fraction("frazione_organica", "frazione_organica_t", "continuous", 0.28),
    Fraction("legno", "legno_t", "semi_rare", 0.90, 0.030),
    Fraction("metallo", "metallo_t", "continuous_tail", 0.65),
    Fraction("plastica", "plastica_t", "continuous", 0.38),
    Fraction("raee", "raee_t", "rare", 1.20, 0.006),
    Fraction("selettiva", "selettiva_t", "rare", 1.00, 0.006),
    Fraction("tessili", "tessili_t", "rare", 1.00, 0.010),
    Fraction("vetro", "vetro_t", "continuous_tail", 0.55),
    Fraction("rifiuti_cd", "rifiuti_cd_t", "rare", 1.35, 0.004),
    Fraction(
        "pulizia_stradale_recupero", "pulizia_stradale_recupero_t", "continuous", 0.35
    ),
]

FRACTION_BY_LABEL = {f.label: f for f in FRACTIONS}


def resolve_annual_col(columns: list[str], fraction: Fraction) -> str | None:
    if fraction.annual_col in columns:
        return fraction.annual_col

    mislabeled_kg_col = fraction.annual_col.removesuffix("_t") + "_kg"
    if mislabeled_kg_col in columns:
        return mislabeled_kg_col

    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Disaggrega un dataset annuo per civico in conferimenti giornalieri, "
            "tenendo conto di calendario di raccolta, stagionalità e rumore stocastico."
        )
    )

    parser.add_argument(
        "--annuo",
        required=True,
        help="CSV annuo per civico prodotto dallo script precedente",
    )
    parser.add_argument("--out", required=True, help="CSV giornaliero dei conferimenti")
    parser.add_argument(
        "--anno", type=int, required=True, help="Anno di produzione da disaggregare"
    )

    parser.add_argument(
        "--calendario",
        default=None,
        help=(
            "CSV calendario. Può essere settimanale: zona_raccolta,frazione,weekday; "
            "oppure esplicito: data,zona_raccolta,frazione,raccolta."
        ),
    )

    parser.add_argument(
        "--annuo-separator", default=",", help="Separatore del CSV annuo"
    )
    parser.add_argument(
        "--calendar-separator", default=",", help="Separatore del CSV calendario"
    )

    parser.add_argument(
        "--id-col", default="id_civico", help="Colonna ID civico nel file annuo"
    )
    parser.add_argument(
        "--zona-col",
        default="zona_raccolta",
        help="Colonna zona raccolta nel file annuo. Se assente, viene usata --zona-default.",
    )
    parser.add_argument(
        "--zona-default",
        default="1",
        help="Zona raccolta usata se il file annuo non ha zona",
    )

    parser.add_argument(
        "--spillover-days",
        type=int,
        default=14,
        help=(
            "Giorni dell'anno successivo considerati per conferire rifiuti prodotti a fine anno. "
            "Serve a evitare residui non conferiti al 31 dicembre."
        ),
    )
    parser.add_argument(
        "--no-force-year-end",
        dest="force_year_end",
        action="store_false",
        help=(
            "Non forza il residuo sull'ultimo giorno dell'anno se manca una raccolta successiva. "
            "Di default il residuo viene forzato a fine anno quando necessario."
        ),
    )
    parser.set_defaults(force_year_end=True)

    parser.add_argument(
        "--missing-calendar-mode",
        choices=["error", "daily"],
        default="error",
        help=(
            "Comportamento se una zona/frazione non è presente nel calendario. "
            "'error' interrompe; 'daily' considera raccolta giornaliera."
        ),
    )

    parser.add_argument(
        "--seed", type=int, default=42, help="Seed pseudo-casuale deterministico"
    )
    parser.add_argument(
        "--round-decimals", type=int, default=3, help="Decimali per i t in output"
    )

    parser.add_argument(
        "--out-produzione",
        default=None,
        help=(
            "Opzionale. Scrive anche il dataset giornaliero di produzione teorica, "
            "molto più grande del file dei conferimenti."
        ),
    )

    return parser.parse_args()


def collect_streaming(lf: pl.LazyFrame) -> pl.DataFrame:
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect(streaming=True)


def sink_csv_streaming(lf: pl.LazyFrame, path: str, separator: str = ",") -> None:
    try:
        lf.sink_csv(path, separator=separator)
    except TypeError:
        collect_streaming(lf).write_csv(path, separator=separator)


def parse_number_expr(col: str) -> pl.Expr:
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()

    return (
        pl.when(s.str.contains(",") & s.str.contains(r"\."))
        .then(s.str.replace_all(r"\.", "").str.replace_all(",", "."))
        .when(s.str.contains(","))
        .then(s.str.replace_all(",", "."))
        .otherwise(s)
        .cast(pl.Float64, strict=False)
        .fill_null(0.0)
    )


def normalize_text(value: object) -> str:
    s = str(value).strip().lower()
    replacements = {
        "à": "a",
        "è": "e",
        "é": "e",
        "ì": "i",
        "ò": "o",
        "ù": "u",
        "\ufeff": "",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    s = s.replace("/", "_")
    return s


def normalize_col_name(value: str) -> str:
    return normalize_text(value).replace("'", "")


def find_col(columns: list[str], candidates: list[str]) -> str | None:
    normalized = {normalize_col_name(c): c for c in columns}
    for candidate in candidates:
        key = normalize_col_name(candidate)
        if key in normalized:
            return normalized[key]
    return None


def expand_fraction_labels(raw_value: object) -> list[str]:
    s = normalize_text(raw_value)

    aliases = {
        "altro_rd": ["altro_rd"],
        "altro": ["altro_rd"],
        "ingombranti": ["ingombranti_misti"],
        "ingombranti_misti": ["ingombranti_misti"],
        "carta": ["carta_cartone"],
        "cartone": ["carta_cartone"],
        "carta_cartone": ["carta_cartone"],
        "carta_e_cartone": ["carta_cartone"],
        "organico": ["frazione_organica"],
        "umido": ["frazione_organica"],
        "frazione_organica": ["frazione_organica"],
        "legno": ["legno"],
        "metallo": ["metallo"],
        "metalli": ["metallo"],
        "plastica": ["plastica"],
        "plastica_metalli": ["plastica", "metallo"],
        "plastica_e_metalli": ["plastica", "metallo"],
        "plastica_metallo": ["plastica", "metallo"],
        "multimateriale": ["plastica", "metallo"],
        "raee": ["raee"],
        "selettiva": ["selettiva"],
        "tessili": ["tessili"],
        "tessile": ["tessili"],
        "abiti": ["tessili"],
        "vetro": ["vetro"],
        "cd": ["rifiuti_cd"],
        "c_d": ["rifiuti_cd"],
        "rifiuti_cd": ["rifiuti_cd"],
        "rifiuti_da_cd": ["rifiuti_cd"],
        "rifiuti_da_c_d": ["rifiuti_cd"],
        "rifiuti_da_costruzione_e_demolizione": ["rifiuti_cd"],
        "pulizia_stradale": ["pulizia_stradale_recupero"],
        "pulizia_stradale_a_recupero": ["pulizia_stradale_recupero"],
        "pulizia_stradale_recupero": ["pulizia_stradale_recupero"],
    }

    if s not in aliases:
        raise ValueError(f"Frazione non riconosciuta nel calendario: {raw_value!r}")

    return aliases[s]


def parse_date_value(value: object) -> date:
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Data non riconosciuta: {value!r}")


def parse_weekday_token(value: object) -> int:
    s = normalize_text(value)

    aliases = {
        "lunedi": 0,
        "lun": 0,
        "monday": 0,
        "mon": 0,
        "martedi": 1,
        "mar": 1,
        "tuesday": 1,
        "tue": 1,
        "mercoledi": 2,
        "mer": 2,
        "wednesday": 2,
        "wed": 2,
        "giovedi": 3,
        "gio": 3,
        "thursday": 3,
        "thu": 3,
        "venerdi": 4,
        "ven": 4,
        "friday": 4,
        "fri": 4,
        "sabato": 5,
        "sab": 5,
        "saturday": 5,
        "sat": 5,
        "domenica": 6,
        "dom": 6,
        "sunday": 6,
        "sun": 6,
    }

    if s in aliases:
        return aliases[s]

    if s.isdigit():
        n = int(s)
        if n == 0:
            return 0
        if 1 <= n <= 7:
            return n - 1

    raise ValueError(f"Giorno della settimana non riconosciuto: {value!r}")


def split_weekday_values(value: object) -> list[int]:
    tokens = re.split(r"[|;/+]", str(value))
    return [parse_weekday_token(t) for t in tokens if str(t).strip()]


def is_truthy(value: object) -> bool:
    s = normalize_text(value)
    return s in {"1", "true", "t", "si", "s", "yes", "y", "x", "ok"}


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def easter_date(year: int) -> date:
    # Algoritmo gregoriano di Meeus/Jones/Butcher.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def italian_holidays(year: int) -> set[date]:
    easter = easter_date(year)
    return {
        date(year, 1, 1),
        date(year, 1, 6),
        easter,
        easter + timedelta(days=1),
        date(year, 4, 25),
        date(year, 5, 1),
        date(year, 6, 2),
        date(year, 8, 15),
        date(year, 11, 1),
        date(year, 12, 8),
        date(year, 12, 25),
        date(year, 12, 26),
    }


def date_features(d: date, start: date, holidays: set[date]) -> dict:
    doy = (d - start).days + 1
    dow = d.weekday()

    is_pre_christmas = d.month == 12 and 15 <= d.day <= 24
    is_christmas_core = d.month == 12 and 24 <= d.day <= 26
    is_post_christmas = (d.month == 12 and d.day >= 27) or (d.month == 1 and d.day <= 6)

    is_summer = d.month in {6, 7, 8}
    is_spring_change = (d.month == 3 and d.day >= 15) or d.month == 4
    is_autumn_change = (
        (d.month == 9 and d.day >= 15)
        or d.month == 10
        or (d.month == 11 and d.day <= 15)
    )

    easter = easter_date(d.year)
    is_easter_window = easter - timedelta(days=2) <= d <= easter + timedelta(days=2)

    return {
        "data": d.isoformat(),
        "day_index": doy - 1,
        "doy": doy,
        "month": d.month,
        "day": d.day,
        "dow": dow,
        "is_weekend": int(dow in {5, 6}),
        "is_saturday": int(dow == 5),
        "is_sunday": int(dow == 6),
        "is_monday": int(dow == 0),
        "is_friday": int(dow == 4),
        "is_holiday": int(d in holidays),
        "is_after_holiday": int((d - timedelta(days=1)) in holidays),
        "is_pre_christmas": int(is_pre_christmas),
        "is_christmas_core": int(is_christmas_core),
        "is_post_christmas": int(is_post_christmas),
        "is_summer": int(is_summer),
        "is_spring_change": int(is_spring_change),
        "is_autumn_change": int(is_autumn_change),
        "is_easter_window": int(is_easter_window),
    }


def build_default_calendar_events(
    zones: list[str], year: int, spillover_days: int
) -> list[dict]:
    start = date(year, 1, 1)
    end = date(year, 12, 31) + timedelta(days=spillover_days)

    # Calendario sintetico minimo. Meglio sostituirlo con quello reale comunale.
    weekdays_by_fraction = {
        "frazione_organica": [0, 2, 5],
        "carta_cartone": [1],
        "plastica": [4],
        "metallo": [4],
        "vetro": [3],
        "altro_rd": [5],
        "ingombranti_misti": [5],
        "legno": [5],
        "raee": [5],
        "selettiva": [5],
        "tessili": [5],
        "rifiuti_cd": [5],
        "pulizia_stradale_recupero": [0, 2, 4],
    }

    events = []
    for z in zones:
        for d in daterange(start, end):
            for fraction, weekdays in weekdays_by_fraction.items():
                if d.weekday() in weekdays:
                    events.append(
                        {
                            "zona_raccolta": str(z),
                            "frazione": fraction,
                            "data": d,
                        }
                    )
    return events


def read_calendar_events(
    path: str | None,
    separator: str,
    zones: list[str],
    year: int,
    spillover_days: int,
) -> list[dict]:
    if path is None:
        print("Nessun calendario fornito: uso un calendario sintetico di esempio.")
        return build_default_calendar_events(zones, year, spillover_days)

    df = pl.read_csv(path, separator=separator, infer_schema_length=0)
    columns = df.columns

    zone_col = find_col(columns, ["zona_raccolta", "zona", "zone"])
    fraction_col = find_col(columns, ["frazione", "rifiuto", "tipo", "tipologia"])
    date_col = find_col(columns, ["data", "date", "giorno"])
    weekday_col = find_col(
        columns, ["weekday", "giorno_settimana", "giorno della settimana"]
    )
    active_col = find_col(columns, ["raccolta", "attivo", "active", "is_collection"])

    if fraction_col is None:
        raise SystemExit("Nel calendario manca una colonna frazione/rifiuto/tipologia.")

    if date_col is None and weekday_col is None:
        raise SystemExit(
            "Il calendario deve avere una colonna 'data' oppure una colonna 'weekday'."
        )

    records = df.to_dicts()

    start = date(year, 1, 1)
    end = date(year, 12, 31)
    end_spill = end + timedelta(days=spillover_days)

    events = []

    if date_col is not None:
        for row in records:
            if active_col is not None and not is_truthy(row.get(active_col)):
                continue

            z = str(row.get(zone_col, "1")).strip() if zone_col else "1"
            d = parse_date_value(row[date_col])

            if not (start <= d <= end_spill):
                continue

            for label in expand_fraction_labels(row[fraction_col]):
                events.append(
                    {
                        "zona_raccolta": z,
                        "frazione": label,
                        "data": d,
                    }
                )

    else:
        all_dates = list(daterange(start, end_spill))

        for row in records:
            if active_col is not None and not is_truthy(row.get(active_col)):
                continue

            z = str(row.get(zone_col, "1")).strip() if zone_col else "1"
            weekdays = split_weekday_values(row[weekday_col])
            labels = expand_fraction_labels(row[fraction_col])

            for d in all_dates:
                if d.weekday() in weekdays:
                    for label in labels:
                        events.append(
                            {
                                "zona_raccolta": z,
                                "frazione": label,
                                "data": d,
                            }
                        )

    if not events:
        raise SystemExit("Il calendario non contiene nessun giorno di raccolta valido.")

    return events


def build_collection_map(
    calendar_events: list[dict],
    zones: list[str],
    fraction: Fraction,
    year: int,
    spillover_days: int,
    missing_calendar_mode: str,
    force_year_end: bool,
) -> pl.DataFrame:
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    end_spill = end + timedelta(days=spillover_days)

    holidays = italian_holidays(year)
    prod_dates = list(daterange(start, end))
    all_dates_with_spillover = list(daterange(start, end_spill))

    by_zone: dict[str, list[date]] = {str(z): [] for z in zones}

    for ev in calendar_events:
        if ev["frazione"] != fraction.label:
            continue
        z = str(ev["zona_raccolta"])
        if z in by_zone:
            by_zone[z].append(ev["data"])

    rows = []

    for z in zones:
        z = str(z)
        collection_dates = sorted(set(by_zone.get(z, [])))

        if not collection_dates:
            if missing_calendar_mode == "daily":
                collection_dates = all_dates_with_spillover
            else:
                raise SystemExit(
                    f"Calendario mancante per zona={z!r}, frazione={fraction.label!r}. "
                    "Aggiungi la riga al calendario oppure usa --missing-calendar-mode daily."
                )

        collection_dates = [d for d in collection_dates if start <= d <= end_spill]

        for idx, d in enumerate(prod_dates):
            j = bisect.bisect_left(collection_dates, d)

            if j < len(collection_dates):
                conferimento_date = collection_dates[j]
            else:
                if force_year_end:
                    conferimento_date = end
                else:
                    raise SystemExit(
                        f"Nessuna raccolta successiva trovata per zona={z!r}, "
                        f"frazione={fraction.label!r}, data={d.isoformat()}. "
                        "Aumenta --spillover-days, aggiungi date calendario nell'anno successivo "
                        "oppure rimuovi --no-force-year-end."
                    )

            feat = date_features(d, start, holidays)
            feat.update(
                {
                    "zona_raccolta": z,
                    "frazione": fraction.label,
                    "conferimento_data": conferimento_date.isoformat(),
                    "raccolta_prevista": int(conferimento_date == d),
                }
            )
            rows.append(feat)

    return pl.DataFrame(rows)


def rnd01(seed: int, salt: int) -> pl.Expr:
    h = pl.struct(
        [
            pl.col("_row_nr").cast(pl.Utf8).alias("_row_nr"),
            pl.col("id_civico").cast(pl.Utf8).fill_null("").alias("id_civico"),
            pl.col("data").cast(pl.Utf8).fill_null("").alias("data"),
            pl.lit(str(salt)).alias("_salt"),
        ]
    ).hash(seed=seed + salt)

    return (((h % 1_000_000).cast(pl.Float64) + 1.0) / 1_000_001.0).clip(
        1e-9, 1.0 - 1e-9
    )


def normal01(seed: int, salt: int) -> pl.Expr:
    u1 = rnd01(seed, salt)
    u2 = rnd01(seed, salt + 20_000)
    return (pl.lit(-2.0) * u1.log()).sqrt() * (
        pl.lit(2.0 * 3.141592653589793) * u2
    ).cos()


def lognormal_multiplier(seed: int, salt: int, sigma: float) -> pl.Expr:
    z = normal01(seed, salt)
    return (pl.lit(sigma) * z - pl.lit(0.5 * sigma * sigma)).exp()


def anchor_day_expr(seed: int, salt: int, n_days: int) -> pl.Expr:
    h = pl.struct(
        [
            pl.col("_row_nr").cast(pl.Utf8).alias("_row_nr"),
            pl.col("id_civico").cast(pl.Utf8).fill_null("").alias("id_civico"),
            pl.lit(str(salt)).alias("_salt_anchor"),
        ]
    ).hash(seed=seed + salt + 99_999)

    return (h % n_days).cast(pl.Int32)


def fraction_salt(label: str, seed: int) -> int:
    digest = hashlib.sha256(f"{seed}:{label}".encode()).digest()
    return int.from_bytes(digest[:4], "big") % 100_000


def seasonal_weekly_expr(fraction: Fraction) -> pl.Expr:
    h = pl.col("is_holiday").cast(pl.Float64)
    after_h = pl.col("is_after_holiday").cast(pl.Float64)
    weekend = pl.col("is_weekend").cast(pl.Float64)
    saturday = pl.col("is_saturday").cast(pl.Float64)
    sunday = pl.col("is_sunday").cast(pl.Float64)
    monday = pl.col("is_monday").cast(pl.Float64)
    friday = pl.col("is_friday").cast(pl.Float64)

    pre_xmas = pl.col("is_pre_christmas").cast(pl.Float64)
    xmas = pl.col("is_christmas_core").cast(pl.Float64)
    post_xmas = pl.col("is_post_christmas").cast(pl.Float64)
    summer = pl.col("is_summer").cast(pl.Float64)
    spring = pl.col("is_spring_change").cast(pl.Float64)
    autumn = pl.col("is_autumn_change").cast(pl.Float64)
    easter = pl.col("is_easter_window").cast(pl.Float64)

    label = fraction.label

    if label == "frazione_organica":
        expr = (
            1.00
            + 0.06 * saturday
            + 0.12 * sunday
            + 0.05 * monday
            + 0.08 * summer
            + 0.18 * h
            + 0.12 * easter
            + 0.12 * xmas
        )

    elif label == "carta_cartone":
        expr = (
            1.00
            + 0.08 * monday
            + 0.05 * friday
            + 0.15 * pre_xmas
            + 0.35 * post_xmas
            + 0.12 * after_h
        )

    elif label == "plastica":
        expr = (
            1.00
            + 0.08 * weekend
            + 0.10 * summer
            + 0.12 * h
            + 0.10 * pre_xmas
            + 0.12 * post_xmas
        )

    elif label == "metallo":
        expr = 1.00 + 0.05 * weekend + 0.06 * summer + 0.08 * h + 0.05 * post_xmas

    elif label == "vetro":
        expr = (
            1.00
            + 0.10 * friday
            + 0.16 * saturday
            + 0.14 * sunday
            + 0.10 * summer
            + 0.20 * h
            + 0.12 * xmas
        )

    elif label == "tessili":
        expr = 1.00 + 0.35 * spring + 0.35 * autumn + 0.05 * weekend

    elif label == "raee":
        expr = 1.00 + 0.35 * post_xmas + 0.15 * autumn + 0.08 * weekend

    elif label in {"ingombranti_misti", "legno"}:
        expr = 1.00 + 0.20 * spring + 0.15 * summer + 0.10 * post_xmas + 0.08 * weekend

    elif label == "rifiuti_cd":
        expr = 1.00 + 0.25 * spring + 0.20 * summer + 0.06 * weekend - 0.10 * xmas

    elif label == "pulizia_stradale_recupero":
        expr = 1.00 + 0.08 * weekend + 0.12 * h + 0.08 * summer + 0.10 * post_xmas

    else:
        expr = 1.00 + 0.05 * weekend + 0.08 * h + 0.06 * summer

    return expr.clip(0.20, 3.00)


def raw_daily_weight_expr(fraction: Fraction, seed: int, n_days: int) -> pl.Expr:
    salt = fraction_salt(fraction.label, seed)
    seasonal = seasonal_weekly_expr(fraction)
    noise = lognormal_multiplier(seed, salt + 1_000, fraction.sigma)

    if fraction.kind in {"rare", "semi_rare"}:
        anchor_day = anchor_day_expr(seed, salt + 2_000, n_days)

        # La probabilità giornaliera viene amplificata nei periodi stagionalmente coerenti,
        # ma rimane limitata per evitare troppi eventi.
        event_probability = (
            pl.lit(fraction.base_event_probability) * seasonal.clip(0.50, 2.50)
        ).clip(0.0005, 0.20)

        event = (pl.col("day_index").cast(pl.Int32) == anchor_day) | (
            rnd01(seed, salt + 3_000) < event_probability
        )

        return pl.when(event).then(seasonal * noise).otherwise(pl.lit(1e-12))

    if fraction.kind == "continuous_tail":
        return seasonal * lognormal_multiplier(seed, salt + 4_000, fraction.sigma)

    return seasonal * noise


def scan_annual_fraction(
    path: str,
    separator: str,
    columns: list[str],
    id_col: str,
    zona_col: str,
    zona_default: str,
    fraction: Fraction,
) -> pl.LazyFrame:
    annual_col = resolve_annual_col(columns, fraction)
    if annual_col is None:
        raise SystemExit(
            f"Nel file annuo manca la colonna {fraction.annual_col!r}. "
            "Lo script si aspetta il formato prodotto dal dataset annuo per civico."
        )

    if id_col not in columns:
        raise SystemExit(f"Nel file annuo manca la colonna ID {id_col!r}.")

    optional_meta = [
        "odonimo",
        "localita",
        "civico",
        "esponente",
        "coord_x",
        "coord_y",
        "densita_locale",
        "cluster_label",
    ]

    select_exprs = [
        pl.col(id_col).cast(pl.Utf8).alias("id_civico"),
    ]

    for c in optional_meta:
        if c in columns:
            select_exprs.append(pl.col(c).alias(c))

    if zona_col in columns:
        select_exprs.append(pl.col(zona_col).cast(pl.Utf8).alias("zona_raccolta"))
    else:
        select_exprs.append(pl.lit(str(zona_default)).alias("zona_raccolta"))

    select_exprs.append(parse_number_expr(annual_col).alias("_annual_t"))

    lf = (
        pl.scan_csv(
            path,
            separator=separator,
            infer_schema_length=0,
            null_values=["", "-", "NULL", "null"],
        )
        .select(select_exprs)
        .with_row_index("_row_nr")
        .filter(pl.col("_annual_t") > 0)
    )

    return lf


def get_annual_columns(path: str, separator: str) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        header = f.readline().strip("\r\n")

    if not header:
        raise SystemExit(f"File annuo vuoto o intestazione mancante: {path}")

    return [col.strip() for col in header.split(separator)]


def get_zones_from_annual(
    path: str,
    separator: str,
    columns: list[str],
    zona_col: str,
    zona_default: str,
) -> list[str]:
    real_zona_col = None

    for c in columns:
        if c.strip() == zona_col:
            real_zona_col = c
            break

    if real_zona_col is None:
        return [str(zona_default)]

    lf = (
        pl.scan_csv(
            path,
            separator=separator,
            infer_schema_length=0,
            null_values=["", "-", "NULL", "null"],
        )
        .select(pl.col(real_zona_col).cast(pl.Utf8).alias("zona_raccolta"))
        .unique()
    )

    zones = collect_streaming(lf)["zona_raccolta"].drop_nulls().to_list()
    return sorted(str(z) for z in zones) or [str(zona_default)]


def merge_part_files(part_files: list[str], final_path: str) -> None:
    with open(final_path, "wb") as out:
        header_written = False

        for part in part_files:
            with open(part, "rb") as f:
                header = f.readline()

                if not header:
                    continue

                if not header_written:
                    out.write(header)
                    header_written = True

                shutil.copyfileobj(f, out)


def process_fraction(
    args: argparse.Namespace,
    fraction: Fraction,
    annual_columns: list[str],
    calendar_events: list[dict],
    zones: list[str],
    part_dir: Path,
    production_part_dir: Path | None,
) -> tuple[str, str | None]:
    print(f"Elaboro frazione: {fraction.label}")

    annual_lf = scan_annual_fraction(
        path=args.annuo,
        separator=args.annuo_separator,
        columns=annual_columns,
        id_col=args.id_col,
        zona_col=args.zona_col,
        zona_default=args.zona_default,
        fraction=fraction,
    )

    collection_map = build_collection_map(
        calendar_events=calendar_events,
        zones=zones,
        fraction=fraction,
        year=args.anno,
        spillover_days=args.spillover_days,
        missing_calendar_mode=args.missing_calendar_mode,
        force_year_end=args.force_year_end,
    )

    n_days = 366 if date(args.anno, 12, 31).timetuple().tm_yday == 366 else 365

    map_lf = collection_map.lazy()

    daily_lf = annual_lf.join(map_lf, on="zona_raccolta", how="inner").with_columns(
        [
            raw_daily_weight_expr(fraction, args.seed, n_days).alias("_raw_weight"),
            pl.lit(args.anno).alias("anno_produzione"),
            pl.lit(fraction.label).alias("frazione"),
        ]
    )

    sums_lf = daily_lf.group_by("_row_nr").agg(pl.sum("_raw_weight").alias("_sum_raw"))

    production_lf = daily_lf.join(sums_lf, on="_row_nr", how="left").with_columns(
        (pl.col("_annual_t") * pl.col("_raw_weight") / pl.col("_sum_raw")).alias(
            "produzione_teorica_t"
        )
    )

    meta_cols = [
        c
        for c in [
            "id_civico",
            "odonimo",
            "localita",
            "civico",
            "esponente",
            "coord_x",
            "coord_y",
            "densita_locale",
            "cluster_label",
        ]
        if c in production_lf.collect_schema().names()
    ]

    group_cols = [
        "_row_nr",
        "_annual_t",
        *meta_cols,
        "zona_raccolta",
        "frazione",
        "conferimento_data",
    ]

    conferimenti_lf = (
        production_lf.group_by(group_cols)
        .agg(
            [
                pl.sum("produzione_teorica_t").alias("conferimento_t"),
                pl.len().alias("giorni_accumulati"),
            ]
        )
        .rename({"conferimento_data": "data"})
        .with_columns(
            [
                pl.lit(args.anno).alias("anno_produzione"),
                pl.col("_annual_t").alias("historical_annual_t"),
                (
                    pl.col("_annual_t") / pl.len().over("_row_nr")
                ).alias("historical_collection_mean_t"),
            ]
        )
    )

    if args.round_decimals >= 0:
        conferimenti_lf = conferimenti_lf.with_columns(
            pl.col("conferimento_t").round(args.round_decimals)
        )

    conferimenti_lf = conferimenti_lf.select(
        [
            "anno_produzione",
            "data",
            *meta_cols,
            "zona_raccolta",
            "frazione",
            "conferimento_t",
            "giorni_accumulati",
            "historical_annual_t",
            "historical_collection_mean_t",
        ]
    )

    part_path = str(part_dir / f"{fraction.label}.csv")
    sink_csv_streaming(conferimenti_lf, part_path)

    production_part_path = None

    if production_part_dir is not None:
        produzione_cols = [
            "anno_produzione",
            "data",
            *meta_cols,
            "zona_raccolta",
            "frazione",
            "produzione_teorica_t",
            "conferimento_data",
            "raccolta_prevista",
        ]

        produzione_lf = production_lf.select(produzione_cols)

        if args.round_decimals >= 0:
            produzione_lf = produzione_lf.with_columns(
                pl.col("produzione_teorica_t").round(args.round_decimals)
            )

        production_part_path = str(production_part_dir / f"{fraction.label}.csv")
        sink_csv_streaming(produzione_lf, production_part_path)

    return part_path, production_part_path


def main() -> None:
    args = parse_args()

    annual_columns = get_annual_columns(args.annuo, args.annuo_separator)

    zones = get_zones_from_annual(
        path=args.annuo,
        separator=args.annuo_separator,
        columns=annual_columns,
        zona_col=args.zona_col,
        zona_default=args.zona_default,
    )

    print(f"Zone raccolta rilevate: {zones}")

    calendar_events = read_calendar_events(
        path=args.calendario,
        separator=args.calendar_separator,
        zones=zones,
        year=args.anno,
        spillover_days=args.spillover_days,
    )

    out_path = Path(args.out)
    part_dir = out_path.with_suffix(out_path.suffix + ".parts")
    part_dir.mkdir(parents=True, exist_ok=True)

    production_part_dir = None
    if args.out_produzione:
        production_out_path = Path(args.out_produzione)
        production_part_dir = production_out_path.with_suffix(
            production_out_path.suffix + ".parts"
        )
        production_part_dir.mkdir(parents=True, exist_ok=True)

    part_files = []
    production_part_files = []

    for fraction in FRACTIONS:
        part, production_part = process_fraction(
            args=args,
            fraction=fraction,
            annual_columns=annual_columns,
            calendar_events=calendar_events,
            zones=zones,
            part_dir=part_dir,
            production_part_dir=production_part_dir,
        )
        part_files.append(part)

        if production_part is not None:
            production_part_files.append(production_part)

    merge_part_files(part_files, args.out)

    if args.out_produzione:
        merge_part_files(production_part_files, args.out_produzione)

    shutil.rmtree(part_dir, ignore_errors=True)

    if production_part_dir is not None:
        shutil.rmtree(production_part_dir, ignore_errors=True)

    print(f"Creato file conferimenti: {args.out}")

    if args.out_produzione:
        print(f"Creato file produzione teorica: {args.out_produzione}")

    print(
        "Nota: il file conferimenti è sparso. Una riga assente equivale a conferimento zero. "
        "Se arrotondi le tonnellate, le somme possono differire leggermente dai totali annui."
    )


if __name__ == "__main__":
    main()
