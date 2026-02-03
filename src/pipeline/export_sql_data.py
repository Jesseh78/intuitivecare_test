from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.utils.fs import data_dir, ensure_dir


def export_sql_ready_files() -> Path:
    """
    Exporta CSVs prontos para carga no PostgreSQL em data/sql_data/.

    Gera:
      - operadoras_ativas.csv              (snake_case, vindo do cadastro normalizado do step2)
      - consolidado_enriquecido.csv        (snake_case)
      - despesas_agregadas.csv             (snake_case)
      - consolidado_despesas.csv           (cópia do step1, para aderência ao enunciado)
    """
    processed = data_dir("processed")
    out_dir = data_dir("sql_data")
    ensure_dir(out_dir)

    # ---------- (A) cópia do consolidado "puro" do step1 (aderência ao enunciado) ----------
    raw_cons_path = processed / "consolidado_despesas.csv"
    if raw_cons_path.exists():
        raw_df = pd.read_csv(raw_cons_path, dtype=str, encoding="utf-8")
        raw_df.to_csv(out_dir / "consolidado_despesas.csv", index=False, encoding="utf-8")

    # ---------- (B) Cadastro de operadoras (preferir o arquivo normalizado do step2) ----------
    cadastro_norm = processed / "operadoras_ativas_normalizado.csv"
    if cadastro_norm.exists():
        cad = pd.read_csv(cadastro_norm, dtype=str, encoding="utf-8")
    else:
        # fallback: tenta derivar do enriquecido (não é o ideal, mas evita quebra)
        enriched_fallback = pd.read_csv(processed / "consolidado_enriquecido.csv", dtype=str, encoding="utf-8")
        cad = (
            enriched_fallback[["CNPJ", "RegistroANS", "Modalidade", "UF"]]
            .drop_duplicates(subset=["CNPJ"])
            .copy()
        )

    # exporta cadastro em snake_case
    cad = cad[["CNPJ", "RegistroANS", "Modalidade", "UF"]].copy()
    cad.columns = ["cnpj", "registro_ans", "modalidade", "uf"]
    cad.to_csv(out_dir / "operadoras_ativas.csv", index=False, encoding="utf-8")

    # ---------- (C) Consolidado enriquecido (SQL) ----------
    enriched = pd.read_csv(processed / "consolidado_enriquecido.csv", dtype=str, encoding="utf-8")
    cons = enriched[
        ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas",
         "RegistroANS", "Modalidade", "UF"]
    ].copy()
    cons.columns = [
        "cnpj", "razao_social", "trimestre", "ano",
        "valor_despesas", "registro_ans", "modalidade", "uf"
    ]
    cons.to_csv(out_dir / "consolidado_enriquecido.csv", index=False, encoding="utf-8")

    # ---------- (D) Agregados ----------
    agg = pd.read_csv(processed / "despesas_agregadas.csv", dtype=str, encoding="utf-8")
    agg = agg[
        ["RazaoSocial", "UF", "TotalDespesas",
         "MediaDespesasTrimestre", "DesvioPadraoDespesas"]
    ].copy()
    agg.columns = [
        "razao_social", "uf", "total_despesas",
        "media_despesas_tri", "desvio_padrao_despesas"
    ]
    agg.to_csv(out_dir / "despesas_agregadas.csv", index=False, encoding="utf-8")

    return out_dir
