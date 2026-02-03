from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.fs import data_dir, ensure_dir


def aggregate_operadora_uf(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa por RazaoSocial e UF e calcula:
      - TotalDespesas
      - MediaDespesasTrimestre
      - DesvioPadraoDespesas
    Observação:
      - Média e desvio padrão são calculados em cima das linhas trimestrais existentes no dataset.
    """
    df = enriched_df.copy()

    # garante float
    df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")

    # chaves de agrupamento
    group_cols = ["RazaoSocial", "UF"]

    agg = (
        df.groupby(group_cols, dropna=False)["ValorDespesas"]
        .agg(
            TotalDespesas="sum",
            MediaDespesasTrimestre="mean",
            DesvioPadraoDespesas="std",
        )
        .reset_index()
    )

    # ordena pelo total desc (estratégia simples e eficiente para esse volume)
    agg = agg.sort_values("TotalDespesas", ascending=False, kind="mergesort").reset_index(drop=True)
    return agg


def save_aggregated_csv(agg_df: pd.DataFrame, filename: str = "despesas_agregadas.csv") -> Path:
    """Salva o CSV agregando em data/processed/."""
    processed_dir = data_dir("processed")
    ensure_dir(processed_dir)
    out_path = processed_dir / filename
    agg_df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path
