from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.utils.fs import data_dir, ensure_dir


def export_sql_ready_files() -> Path:
    """
    Exporta CSVs prontos para carga no PostgreSQL (snake_case e tipos coerentes).

    Gera a pasta:
      data/sql_data/

    Arquivos gerados:
      - operadoras_ativas.csv
      - consolidado_enriquecido.csv
      - despesas_agregadas.csv
    """
    processed = data_dir("processed")
    out_dir = data_dir("sql_data")
    ensure_dir(out_dir)

    # ---------- Operadoras ativas ----------
    enriched = pd.read_csv(
        processed / "consolidado_enriquecido.csv",
        dtype=str,
        encoding="utf-8",
    )

    operadoras = (
        enriched[["CNPJ", "RegistroANS", "Modalidade", "UF"]]
        .drop_duplicates(subset=["CNPJ"])
        .copy()
    )
    operadoras.columns = ["cnpj", "registro_ans", "modalidade", "uf"]
    operadoras.to_csv(
        out_dir / "operadoras_ativas.csv",
        index=False,
        encoding="utf-8",
    )

    # ---------- Consolidado enriquecido ----------
    consolidado = enriched[
        ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas",
         "RegistroANS", "Modalidade", "UF"]
    ].copy()
    consolidado.columns = [
        "cnpj", "razao_social", "trimestre", "ano",
        "valor_despesas", "registro_ans", "modalidade", "uf"
    ]
    consolidado.to_csv(
        out_dir / "consolidado_enriquecido.csv",
        index=False,
        encoding="utf-8",
    )

    # ---------- Agregados ----------
    agregados = pd.read_csv(
        processed / "despesas_agregadas.csv",
        dtype=str,
        encoding="utf-8",
    )
    agregados = agregados[
        ["RazaoSocial", "UF", "TotalDespesas",
         "MediaDespesasTrimestre", "DesvioPadraoDespesas"]
    ].copy()
    agregados.columns = [
        "razao_social", "uf", "total_despesas",
        "media_despesas_tri", "desvio_padrao_despesas"
    ]
    agregados.to_csv(
        out_dir / "despesas_agregadas.csv",
        index=False,
        encoding="utf-8",
    )

    return out_dir
