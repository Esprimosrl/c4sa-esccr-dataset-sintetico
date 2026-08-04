#!/usr/bin/env python3

import argparse
import math
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class Fraction:
    input_col: str
    output_col: str
    kind: str


FRACTIONS = [
    Fraction("Altro RD", "altro_rd_t", "continuous"),
    Fraction("Ingombranti misti", "ingombranti_misti_t", "rare"),
    Fraction("Carta e cartone", "carta_cartone_t", "continuous"),
    Fraction("Frazione Organica", "frazione_organica_t", "continuous"),
    Fraction("Legno", "legno_t", "semi_rare"),
    Fraction("Metallo", "metallo_t", "continuous_tail"),
    Fraction("Plastica", "plastica_t", "continuous"),
    Fraction("RAEE", "raee_t", "rare"),
    Fraction("Selettiva", "selettiva_t", "rare"),
    Fraction("Tessili", "tessili_t", "rare"),
    Fraction("Vetro", "vetro_t", "continuous"),
    Fraction("Rifiuti da C&D", "rifiuti_cd_t", "rare"),
    Fraction("Pulizia stradale a recupero", "pulizia_stradale_recupero_t", "street"),
]


ANNCSU_METADATA = {
    "CODICE_COMUNE": "anncsu_codice_comune",
    "CODICE_ISTAT": "anncsu_codice_istat",
    "PROGRESSIVO_NAZIONALE": "anncsu_progressivo_nazionale",
    "CODICE_COMUNALE": "anncsu_codice_comunale",
    "DIZIONE_LINGUA1": "anncsu_dizione_lingua1",
    "DIZIONE_LINGUA2": "anncsu_dizione_lingua2",
    "CODICE_COMUNALE_ACCESSO": "anncsu_codice_comunale_accesso",
    "SPECIFICITA": "anncsu_specificita",
    "METRICO": "anncsu_metrico",
    "PROGRESSIVO_SNC": "anncsu_progressivo_snc",
    "QUOTA": "anncsu_quota",
    "METODO": "anncsu_metodo",
}

# conservative civic bounds for the synthetic residential model;
# replace them with calibrated limits when operator observations become available.
MAX_ANNUAL_T = {
    "altro_rd_t": 0.10,
    "ingombranti_misti_t": 0.30,
    "carta_cartone_t": 0.50,
    "frazione_organica_t": 1.00,
    "legno_t": 0.15,
    "metallo_t": 0.05,
    "plastica_t": 0.25,
    "raee_t": 0.10,
    "selettiva_t": 0.01,
    "tessili_t": 0.12,
    "vetro_t": 0.30,
    "rifiuti_cd_t": 0.25,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Distribuisce i totali annui dei rifiuti sui civici, assumendo civici residenziali."
    )
    parser.add_argument("--civici", required=True, help="CSV civici, separatore ';'")
    parser.add_argument(
        "--totali", required=True, help="CSV totali rifiuti, separatore ',' di default"
    )
    parser.add_argument("--out", required=True, help="CSV annuale di output")
    parser.add_argument(
        "--anno",
        type=int,
        default=None,
        help="Anno da usare. Se omesso usa il massimo anno presente.",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Seed per pseudo-random deterministico"
    )
    parser.add_argument(
        "--grid-m", type=float, default=120.0, help="Lato cella griglia in metri"
    )
    parser.add_argument(
        "--totali-separator", default=",", help="Separatore CSV del file dei totali"
    )
    parser.add_argument(
        "--round-decimals",
        type=int,
        default=-1,
        help="Decimali per le tonnellate in output. Usa -1 per non arrotondare.",
    )
    return parser.parse_args()


def parse_eu_number(value) -> float:
    if value is None:
        return 0.0

    s = str(value).strip()
    if s == "" or s == "-":
        return 0.0

    s = s.replace("\u00a0", "").replace(" ", "")

    if "," in s and "." in s:
        # Esempio: 5.341,740 -> 5341.740
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Esempio: 670,853 -> 670.853
        s = s.replace(",", ".")

    return float(s)


def parse_number_expr(col: str) -> pl.Expr:
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()

    return (
        pl.when(s.str.contains(",") & s.str.contains(r"\."))
        .then(s.str.replace_all(r"\.", "").str.replace_all(",", "."))
        .when(s.str.contains(","))
        .then(s.str.replace_all(",", "."))
        .otherwise(s)
        .cast(pl.Float64, strict=False)
    )


def read_totals(
    path: str, separator: str, anno: int | None
) -> tuple[int, dict[str, float]]:
    try:
        df = pl.read_csv(
            path,
            separator=separator,
            infer_schema_length=0,
            null_values=["", "-"],
        )
    except Exception as exc:
        raise SystemExit(
            "Errore leggendo il file dei totali. Se il separatore è ',' e i numeri "
            "usano la virgola decimale, i campi numerici devono essere tra virgolette. "
            "In alternativa usa un separatore diverso, per esempio ';'.\n"
            f"Errore originale: {exc}"
        )

    df = df.rename({c: c.strip().replace("\ufeff", "") for c in df.columns})

    if "Anno" not in df.columns:
        raise SystemExit("Nel file dei totali manca la colonna 'Anno'.")

    missing = [f.input_col for f in FRACTIONS if f.input_col not in df.columns]
    if missing:
        raise SystemExit(f"Nel file dei totali mancano queste colonne: {missing}")

    df = df.with_columns(
        pl.col("Anno")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Int64, strict=False)
        .alias("_anno")
    )

    if anno is None:
        anno = df.select(pl.max("_anno")).item()
        if anno is None:
            raise SystemExit("Impossibile determinare l'anno dal file dei totali.")

    row_df = df.filter(pl.col("_anno") == anno)

    if row_df.height == 0:
        raise SystemExit(f"Anno {anno} non trovato nel file dei totali.")

    row = row_df.to_dicts()[0]

    targets_t = {f.output_col: parse_eu_number(row.get(f.input_col)) for f in FRACTIONS}

    return anno, targets_t


def scan_civici_selected(path: str) -> pl.LazyFrame:
    metadata_exprs = [
        pl.col(source).cast(pl.Utf8).alias(target)
        for source, target in ANNCSU_METADATA.items()
    ]

    return (
        pl.scan_csv(
            path,
            separator=";",
            has_header=True,
            infer_schema_length=0,
            null_values=["", "-", "NULL", "null"],
        )
        .select(
            [
                pl.col("PROGRESSIVO_ACCESSO").cast(pl.Utf8).alias("id_civico"),
                pl.col("ODONIMO").cast(pl.Utf8).alias("odonimo"),
                pl.col("LOCALITA'").cast(pl.Utf8).alias("localita"),
                pl.col("CIVICO").cast(pl.Utf8).alias("civico"),
                pl.col("ESPONENTE").cast(pl.Utf8).alias("esponente"),
                parse_number_expr("COORD_X_COMUNE").alias("coord_x"),
                parse_number_expr("COORD_Y_COMUNE").alias("coord_y"),
                *metadata_exprs,
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("id_civico").is_null() | (pl.col("id_civico") == ""))
                .then(
                    pl.concat_str(
                        [
                            pl.col("odonimo").fill_null(""),
                            pl.col("civico").fill_null(""),
                            pl.col("esponente").fill_null(""),
                            pl.col("coord_x").cast(pl.Utf8).fill_null(""),
                            pl.col("coord_y").cast(pl.Utf8).fill_null(""),
                        ],
                        separator="|",
                    )
                )
                .otherwise(pl.col("id_civico"))
                .alias("id_civico")
            ]
        )
    )


def detect_lonlat(stats: dict) -> bool:
    min_x = stats.get("min_x")
    max_x = stats.get("max_x")
    min_y = stats.get("min_y")
    max_y = stats.get("max_y")

    if None in (min_x, max_x, min_y, max_y):
        return False

    return (
        -180.0 <= min_x <= 180.0
        and -180.0 <= max_x <= 180.0
        and -90.0 <= min_y <= 90.0
        and -90.0 <= max_y <= 90.0
    )


def add_spatial_columns(
    lf: pl.LazyFrame, grid_m: float, is_lonlat: bool, lat0: float
) -> pl.LazyFrame:
    if is_lonlat:
        # Conversione equirettangolare approssimata: sufficiente per stimare densità locale urbana.
        x_scale = 111_320.0 * math.cos(math.radians(lat0))
        y_scale = 111_320.0
    else:
        # Coordinate già metriche o proiettate.
        x_scale = 1.0
        y_scale = 1.0

    return lf.with_columns(
        [
            (pl.col("coord_x") * x_scale).alias("_x_m"),
            (pl.col("coord_y") * y_scale).alias("_y_m"),
        ]
    ).with_columns(
        [
            (pl.col("_x_m") / grid_m)
            .floor()
            .cast(pl.Int64, strict=False)
            .alias("cell_x"),
            (pl.col("_y_m") / grid_m)
            .floor()
            .cast(pl.Int64, strict=False)
            .alias("cell_y"),
        ]
    )


def make_density_map(lf: pl.LazyFrame) -> tuple[pl.DataFrame, float]:
    valid = pl.col("cell_x").is_not_null() & pl.col("cell_y").is_not_null()

    cell_counts = (
        lf.filter(valid)
        .group_by(["cell_x", "cell_y"])
        .agg(pl.len().alias("n_cell"))
        .collect(engine="streaming")
    )

    if cell_counts.height == 0:
        density_map = pl.DataFrame(
            schema={
                "cell_x": pl.Int64,
                "cell_y": pl.Int64,
                "densita_locale": pl.UInt32,
            }
        )
        return density_map, 1.0

    offsets = pl.DataFrame(
        {
            "dx": [-1, -1, -1, 0, 0, 0, 1, 1, 1],
            "dy": [-1, 0, 1, -1, 0, 1, -1, 0, 1],
        }
    )

    # Densità smussata: numero di civici nella cella e nelle 8 celle adiacenti.
    density_map = (
        cell_counts.join(offsets, how="cross")
        .with_columns(
            [
                (pl.col("cell_x") + pl.col("dx")).alias("_target_cell_x"),
                (pl.col("cell_y") + pl.col("dy")).alias("_target_cell_y"),
            ]
        )
        .group_by(["_target_cell_x", "_target_cell_y"])
        .agg(pl.sum("n_cell").alias("densita_locale"))
        .rename({"_target_cell_x": "cell_x", "_target_cell_y": "cell_y"})
        .select(["cell_x", "cell_y", "densita_locale"])
    )

    p90 = density_map.select(
        pl.col("densita_locale").quantile(0.90, interpolation="nearest")
    ).item()

    p90 = max(float(p90 or 1.0), 1.0)
    return density_map, p90


def rnd01(seed: int, salt: int) -> pl.Expr:
    h = pl.struct(
        [
            pl.col("id_civico").cast(pl.Utf8).fill_null("").alias("id_civico"),
            pl.col("odonimo").cast(pl.Utf8).fill_null("").alias("odonimo"),
            pl.col("civico").cast(pl.Utf8).fill_null("").alias("civico"),
            pl.lit(str(salt)).alias("_salt"),
        ]
    ).hash(seed=seed + salt)

    return (((h % 1_000_000).cast(pl.Float64) + 1.0) / 1_000_001.0).clip(
        1e-9, 1.0 - 1e-9
    )


def normal01(seed: int, salt: int) -> pl.Expr:
    u1 = rnd01(seed, salt)
    u2 = rnd01(seed, salt + 10_000)

    return (pl.lit(-2.0) * u1.log()).sqrt() * (pl.lit(2.0 * math.pi) * u2).cos()


def lognormal_multiplier(seed: int, salt: int, sigma: float) -> pl.Expr:
    z = normal01(seed, salt)
    # -0.5*sigma^2 mantiene circa media 1.
    return (pl.lit(sigma) * z - pl.lit(0.5 * sigma * sigma)).exp()


def rare_weight(
    base: pl.Expr,
    seed: int,
    salt: int,
    probability: pl.Expr,
    sigma: float,
    min_weight: float = 0.25,
    max_weight: float = 4.0,
) -> pl.Expr:
    event = rnd01(seed, salt + 20_000) < probability.clip(0.001, 0.95)
    raw = base * lognormal_multiplier(seed, salt, sigma)
    bounded = min_weight + (max_weight - min_weight) * raw / (1.0 + raw)
    return pl.when(event).then(bounded).otherwise(pl.lit(0.0))


def add_raw_weights(
    lf: pl.LazyFrame, density_map: pl.DataFrame, density_p90: float, seed: int
) -> pl.LazyFrame:
    density_lf = density_map.lazy()

    lf = (
        lf.join(density_lf, on=["cell_x", "cell_y"], how="left")
        .with_columns(
            [
                pl.col("densita_locale")
                .fill_null(1)
                .cast(pl.Float64)
                .alias("densita_locale")
            ]
        )
        .with_columns(
            [
                (
                    (
                        (pl.col("densita_locale") + 1.0).log()
                        / math.log(density_p90 + 1.0)
                    ).clip(0.15, 2.0)
                ).alias("_density_score")
            ]
        )
        .with_columns(
            [
                # Proxy del numero atteso di abitazioni/persone dietro un civico.
                # In aree dense, un civico è più probabilmente un condominio o un edificio plurifamiliare.
                (0.45 + 0.85 * pl.col("_density_score")).alias("_res_base"),
                (1.25 - pl.col("_density_score"))
                .clip(0.0, 1.0)
                .alias("_isolated_score"),
            ]
        )
    )

    d = pl.col("_density_score")
    isolated = pl.col("_isolated_score")
    base = pl.col("_res_base") * lognormal_multiplier(seed, 1, 0.35)

    raw_exprs = []

    for idx, f in enumerate(FRACTIONS, start=1):
        salt = idx * 1000

        if f.kind == "continuous":
            expr = (base * lognormal_multiplier(seed, salt, 0.45)).clip(0.20, 4.0)

        elif f.kind == "continuous_tail":
            expr = (
                base * (0.90 + 0.35 * isolated) * lognormal_multiplier(seed, salt, 0.90)
            ).clip(0.15, 5.0)

        elif f.kind == "street":
            # La pulizia stradale non è davvero del singolo civico.
            # La assegniamo di più alle aree dense, approssimando strade centrali e commerciali.
            expr = (0.20 + d * d) * lognormal_multiplier(seed, salt, 0.55)

        elif f.kind == "semi_rare":
            probability = 0.38 + 0.08 * d + 0.04 * isolated
            expr = rare_weight(
                base=base * (0.90 + 0.50 * isolated),
                seed=seed,
                salt=salt,
                probability=probability,
                sigma=0.45,
            )

        elif f.kind == "rare":
            if f.output_col == "rifiuti_cd_t":
                probability = 0.18 + 0.05 * d + 0.04 * isolated
                sigma = 0.50
                rare_base = base * (0.80 + 0.60 * isolated)
            elif f.output_col == "ingombranti_misti_t":
                probability = 0.72 + 0.08 * d
                sigma = 0.45
                rare_base = base
            elif f.output_col == "raee_t":
                probability = 0.38 + 0.08 * d
                sigma = 0.40
                rare_base = base
            elif f.output_col == "tessili_t":
                probability = 0.58 + 0.10 * d
                sigma = 0.40
                rare_base = base
            else:
                probability = 0.20 + 0.06 * d
                sigma = 0.40
                rare_base = base

            expr = rare_weight(
                base=rare_base,
                seed=seed,
                salt=salt,
                probability=probability,
                sigma=sigma,
            )

        else:
            raise ValueError(f"Tipo frazione non gestito: {f.kind}")

        raw_exprs.append(expr.alias(f"_raw_{f.output_col}"))

    return lf.with_columns(raw_exprs)


def normalize_to_targets(
    lf_raw: pl.LazyFrame, targets_t: dict[str, float]
) -> pl.LazyFrame:
    raw_cols = [f"_raw_{f.output_col}" for f in FRACTIONS]

    sums = (
        lf_raw.select([pl.sum(c).alias(c) for c in raw_cols])
        .collect(engine="streaming")
        .row(0, named=True)
    )

    out_exprs = []

    for f in FRACTIONS:
        raw_col = f"_raw_{f.output_col}"
        raw_sum = float(sums[raw_col] or 0.0)
        target = float(targets_t[f.output_col])

        if target == 0.0:
            scale = 0.0
        elif raw_sum <= 0.0:
            raise SystemExit(
                f"Somma grezza nulla per {f.output_col}; impossibile normalizzare."
            )
        else:
            scale = target / raw_sum

        out_exprs.append((pl.col(raw_col) * scale).alias(f.output_col))

    return lf_raw.with_columns(out_exprs)


def main() -> None:
    args = parse_args()

    anno, targets_t = read_totals(
        path=args.totali,
        separator=args.totali_separator,
        anno=args.anno,
    )

    lf0 = scan_civici_selected(args.civici)

    coord_stats = (
        lf0.filter(pl.col("coord_x").is_not_null() & pl.col("coord_y").is_not_null())
        .select(
            [
                pl.min("coord_x").alias("min_x"),
                pl.max("coord_x").alias("max_x"),
                pl.min("coord_y").alias("min_y"),
                pl.max("coord_y").alias("max_y"),
                pl.mean("coord_y").alias("mean_y"),
            ]
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )

    is_lonlat = detect_lonlat(coord_stats)
    lat0 = float(coord_stats.get("mean_y") or 0.0)

    lf_spatial = add_spatial_columns(
        lf=lf0,
        grid_m=args.grid_m,
        is_lonlat=is_lonlat,
        lat0=lat0,
    )

    density_map, density_p90 = make_density_map(lf_spatial)

    lf_raw = add_raw_weights(
        lf=lf_spatial,
        density_map=density_map,
        density_p90=density_p90,
        seed=args.seed,
    )

    lf_norm = normalize_to_targets(lf_raw, targets_t)

    waste_cols = [f.output_col for f in FRACTIONS]

    metadata_cols = list(ANNCSU_METADATA.values())

    lf_out = lf_norm.with_columns(
        [
            pl.lit(anno).alias("anno"),
            pl.sum_horizontal([pl.col(c) for c in waste_cols]).alias("totale_t"),
        ]
    ).select(
        [
            "anno",
            "id_civico",
            "odonimo",
            "localita",
            "civico",
            "esponente",
            "coord_x",
            "coord_y",
            "densita_locale",
            *metadata_cols,
            *waste_cols,
            "totale_t",
        ]
    )

    stats_t = (
        lf_out.select(
            [pl.sum(c).alias(f"sum_{c}") for c in waste_cols]
            + [pl.max(c).alias(f"max_{c}") for c in waste_cols]
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    mismatches = [
        f"{col}: {stats_t[f'sum_{col}']} != {target}"
        for col, target in targets_t.items()
        if not math.isclose(
            float(stats_t[f"sum_{col}"]), target, rel_tol=1e-10, abs_tol=1e-8
        )
    ]
    if mismatches:
        raise SystemExit("Riconciliazione ISPRA fallita: " + "; ".join(mismatches))

    excessive = [
        f"{col}: {stats_t[f'max_{col}']:.6f} t > {limit:.6f} t"
        for col, limit in MAX_ANNUAL_T.items()
        if float(stats_t[f"max_{col}"]) > limit
    ]
    if excessive:
        raise SystemExit("Limite annuale per civico superato: " + "; ".join(excessive))

    if args.round_decimals >= 0:
        t_cols = [*waste_cols, "totale_t"]
        lf_out = lf_out.with_columns(
            [pl.col(c).round(args.round_decimals).alias(c) for c in t_cols]
        )

    try:
        lf_out.sink_csv(args.out, separator=",")
    except TypeError:
        # Fallback per versioni Polars con firma diversa di sink_csv.
        # Meno robusto su file enormi, ma mantiene l'esecuzione streaming nella collect.
        lf_out.collect(engine="streaming").write_csv(args.out, separator=",")

    print(f"Creato: {args.out}")
    print(f"Anno: {anno}")
    print(f"Coordinate interpretate come lon/lat: {is_lonlat}")
    print(f"Grid: {args.grid_m} m")
    print(
        "Nota: se hai arrotondato le tonnellate, le somme finali possono differire di poche tonnellate dai target."
    )


if __name__ == "__main__":
    main()
