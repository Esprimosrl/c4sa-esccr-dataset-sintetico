import json
import subprocess
import sys
from pathlib import Path

import polars as pl

from dataset.distribuzione_annuale import rare_weight, read_totals
from dataset.distribuzione_giornaliera import (
    FRACTIONS,
    FRACTION_BY_LABEL,
    REQUIRED_STATIC_COLUMNS,
    scan_annual_fraction,
)


def test_ispra_totals_remain_tonnes(tmp_path):
    path = tmp_path / "totali.csv"
    pl.DataFrame(
        {
            "Anno": [2024],
            "Altro RD": ["670.853"],
            "Ingombranti misti": ["5341.740"],
            "Carta e cartone": ["5065.937"],
            "Frazione Organica": ["11892.390"],
            "Legno": ["1551.400"],
            "Metallo": ["255.547"],
            "Plastica": ["2126.822"],
            "RAEE": ["192.204"],
            "Selettiva": ["39.962"],
            "Tessili": ["1596.350"],
            "Vetro": ["2569.613"],
            "Rifiuti da C&D": ["1122.220"],
            "Pulizia stradale a recupero": ["0.000"],
        }
    ).write_csv(path)

    _, totals = read_totals(str(path), ",", 2024)

    assert totals["altro_rd_t"] == 670.853
    assert totals["frazione_organica_t"] == 11892.390


def test_daily_input_keeps_static_metadata_only(tmp_path):
    path = tmp_path / "annuale.parquet"
    pl.DataFrame(
        {
            "anno": [2024],
            "id_civico": ["1"],
            "zona": [1],
            "zona_raccolta": [1],
            "zona_costruzione": ["B3.4"],
            "prg_norme": ["4.1, 4.2"],
            "altro_rd_t": [1.5],
            "carta_cartone_t": [2.5],
            "totale_t": [4.0],
        }
    ).write_parquet(path)

    result = scan_annual_fraction(
        str(path),
        ",",
        pl.scan_parquet(path).collect_schema().names(),
        "id_civico",
        "zona_raccolta",
        "1",
        FRACTION_BY_LABEL["altro_rd"],
    ).collect()

    assert "zona_costruzione" in result.columns
    assert "prg_norme" in result.columns
    assert "altro_rd_t" not in result.columns
    assert "carta_cartone_t" not in result.columns
    assert "totale_t" not in result.columns


def test_rare_weights_use_real_zeros_and_bounded_positive_values():
    frame = pl.DataFrame(
        {
            "id_civico": [str(i) for i in range(5000)],
            "odonimo": ["VIA TEST"] * 5000,
            "civico": [str(i) for i in range(5000)],
        }
    )

    values = frame.select(
        rare_weight(pl.lit(1.0), 42, 1000, pl.lit(0.05), 0.45).alias("w")
    )["w"]

    assert (values == 0).any()
    assert values.filter(values > 0).min() >= 0.25
    assert values.max() < 4.0


def test_ml_scenario_runner_aggregates_reconciles_and_removes_raw_file(tmp_path):
    annual_path = tmp_path / "annual.parquet"
    calendar_path = tmp_path / "calendar.csv"
    out_dir = tmp_path / "scenarios"

    annual = {column: [""] for column in REQUIRED_STATIC_COLUMNS}
    annual.update(
        {
            "anno": [2024],
            "id_civico": ["1"],
            "zona_raccolta": ["1"],
            "zona_costruzione": ["B"],
            **{fraction.annual_col: [0.0] for fraction in FRACTIONS},
            "totale_t": [0.001],
        }
    )
    annual["altro_rd_t"] = [0.001]
    pl.DataFrame(annual).write_parquet(annual_path)
    pl.DataFrame(
        {
            "zona_raccolta": ["1"] * len(FRACTIONS),
            "frazione": [fraction.label for fraction in FRACTIONS],
            "weekday": ["lunedi"] * len(FRACTIONS),
        }
    ).write_csv(calendar_path)

    root = Path(__file__).resolve().parent
    subprocess.run(
        [
            sys.executable,
            str(root / "dataset" / "genera_scenari_ml.py"),
            "--annuo",
            str(annual_path),
            "--calendario",
            str(calendar_path),
            "--out-dir",
            str(out_dir),
            "--seed-inizio",
            "42",
            "--seed-fine",
            "42",
            "--spillover-days",
            "0",
            "--missing-calendar-mode",
            "error",
            "--python",
            sys.executable,
        ],
        cwd=root,
        check=True,
    )

    scenario_dir = out_dir / "scenario_seed=42"
    out_path = scenario_dir / "ritiri_zona.parquet"
    result = pl.read_parquet(out_path)
    assert result["scenario_seed"].unique().to_list() == [42]
    assert (
        result.select(
            pl.struct(
                ["scenario_seed", "data", "zona_raccolta", "frazione"]
            ).n_unique()
        ).item()
        == result.height
    )
    assert result["conferimento_t"].sum() == 0.001
    assert (result["conferimento_t"] == 0).any()
    assert not (scenario_dir / ".conferimenti_civici.parquet").exists()

    manifest = json.loads((scenario_dir / "manifest.json").read_text())
    assert manifest["annual_seed"] == 42
    assert manifest["scenario_seed"] == 42
    assert manifest["dataset_sha256"]
