# src/pipeline/validate_enrich.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import requests

from src.utils.fs import data_dir, ensure_dir


StrategyInvalidCnpj = Literal["drop", "keep_mark"]


@dataclass(frozen=True)
class Step2Outputs:
    enriched_csv: Path
    aggregated_csv: Path | None
    cadastro_csv: Path


@dataclass(frozen=True)
class CadastroDownloadResult:
    csv_path: Path


# -------------------------
# Normalização e Validação
# -------------------------

_CNPJ_NON_DIGITS = re.compile(r"\D+")


def normalize_cnpj(value: str | int | float | None) -> str:
    """
    Normaliza CNPJ para 14 dígitos (string):
    - remove tudo que não é dígito
    - se ficar < 14, faz left-pad com zeros
    - se ficar > 14, corta para 14
    """
    if value is None:
        return ""
    s = str(value)
    digits = _CNPJ_NON_DIGITS.sub("", s)
    if not digits:
        return ""
    return digits.zfill(14)[:14]


def _all_equal_digits(cnpj: str) -> bool:
    return len(cnpj) == 14 and len(set(cnpj)) == 1


def is_valid_cnpj(cnpj: str) -> bool:
    """
    Valida DV do CNPJ (14 dígitos).
    """
    if not cnpj or len(cnpj) != 14 or not cnpj.isdigit():
        return False
    if _all_equal_digits(cnpj):
        return False

    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    nums = [int(x) for x in cnpj]

    s1 = sum(a * b for a, b in zip(nums[:12], weights1))
    d1 = 11 - (s1 % 11)
    d1 = 0 if d1 >= 10 else d1

    s2 = sum(a * b for a, b in zip(nums[:13], weights2))
    d2 = 11 - (s2 % 11)
    d2 = 0 if d2 >= 10 else d2

    return nums[12] == d1 and nums[13] == d2


def _normalize_razao_social(series: pd.Series) -> pd.Series:
    """
    Garante RazaoSocial como string.
    Valores numéricos puros (registro ANS) são mantidos para posterior enriquecimento.
    """
    s = series.astype(str).str.strip()
    # normaliza valores "nan" para vazio
    s = s.replace({"nan": "", "None": ""})
    return s


# -------------------------
# Download / Leitura Cadastro
# -------------------------

def _fetch_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def _download_file(url: str, dst: Path, timeout: int = 60) -> None:
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    with dst.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 512):
            if chunk:
                f.write(chunk)


def download_latest_cadastro_csv(cadastro_dir_url: str) -> CadastroDownloadResult:
    """
    Baixa o CSV mais recente do diretório público da ANS (operadoras ativas).

    Importante: para deixar o pipeline reprodutível (e evitar falhas de rede/SSL),
    usamos cache local em data/reference/operadoras_ativas.csv.
    Se o download falhar, reaproveitamos o arquivo local, se existir.
    """
    reference_dir = data_dir("reference")
    ensure_dir(reference_dir)

    cached = reference_dir / "operadoras_ativas.csv"

    try:
        html = _fetch_text(cadastro_dir_url)

        # captura hrefs que terminem com .csv
        csv_links = re.findall(r'href="([^"]+\.csv)"', html, flags=re.IGNORECASE)
        if not csv_links:
            raise RuntimeError("Não encontrei CSVs no diretório de cadastro informado.")

        csv_links = sorted(set(csv_links))
        chosen = csv_links[-1]

        if chosen.startswith("http"):
            csv_url = chosen
        else:
            base = cadastro_dir_url.rstrip("/") + "/"
            csv_url = base + chosen.lstrip("/")

        _download_file(csv_url, cached)
        return CadastroDownloadResult(csv_path=cached)

    except (requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:
        # fallback para cache local
        if cached.exists() and cached.stat().st_size > 0:
            print(f"[WARN] Falha ao baixar cadastro ANS ({type(e).__name__}). Usando cache local: {cached}")
            return CadastroDownloadResult(csv_path=cached)
        raise


def _load_cadastro(path: Path) -> pd.DataFrame:
    """
    Lê cadastro (CSV) e retorna colunas mínimas:
      CNPJ, RegistroANS, Modalidade, UF

    O cadastro da ANS vem normalmente com separador ';' e encoding Windows-1252/latin-1.
    """
    # dtype=str para preservar zeros à esquerda
    # sep=';' pois o arquivo usa ponto-e-vírgula
    # encoding latin-1 / cp1252 para evitar erro de decode
    cad = pd.read_csv(
        path,
        dtype=str,
        encoding="latin-1",
        sep=";",
        engine="python",
        on_bad_lines="skip",
    )

    cad.columns = [c.strip() for c in cad.columns]

    # Mapeia para os nomes internos do seu pipeline
    # Header real tem: REGISTRO_OPERADORA;CNPJ;Razao_Social;...;Modalidade;...;UF;...
    colmap = {}
    for c in cad.columns:
        lc = c.lower().strip()

        if lc == "cnpj":
            colmap[c] = "CNPJ"
        elif lc in ("registro_operadora", "registro_operadora_ans", "registro"):
            colmap[c] = "RegistroANS"
        elif "modalidade" in lc:
            colmap[c] = "Modalidade"
        elif lc == "uf":
            colmap[c] = "UF"

    cad = cad.rename(columns=colmap)

    required = ["CNPJ", "RegistroANS", "Modalidade", "UF"]
    missing = [c for c in required if c not in cad.columns]
    if missing:
        raise RuntimeError(
            f"Cadastro não possui colunas esperadas: {missing}. "
            f"Colunas encontradas: {list(cad.columns)}"
        )

    cad = cad[required].copy()

    # Normaliza CNPJ para 14 dígitos
    cad["CNPJ"] = cad["CNPJ"].apply(normalize_cnpj)

    # Remove linhas com CNPJ vazio
    cad = cad[cad["CNPJ"].str.len() == 14].copy()

    return cad


def _dedupe_cadastro(cad: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicação simples por CNPJ:
    - mantém a primeira ocorrência não-nula (padrão pandas)
    """
    # se houver duplicatas, mantém a primeira
    cad_best = cad.drop_duplicates(subset=["CNPJ"], keep="first").copy()
    return cad_best


# -------------------------
# Step2 principal
# -------------------------

def validate_and_enrich(
    consolidated_csv: Path,
    cadastro_dir_url: str,
    strategy_invalid_cnpj: StrategyInvalidCnpj = "keep_mark",
) -> tuple[pd.DataFrame, Step2Outputs]:
    """
    Step2:
    - valida CNPJ (DV), valor positivo, razão social não vazia
    - baixa cadastro de operadoras ativas e enriquece (RegistroANS, Modalidade, UF)
    - salva:
        data/processed/consolidado_enriquecido.csv
        data/processed/operadoras_ativas_normalizado.csv   (cadastro deduplicado/normalizado)
    """
    processed_dir = data_dir("processed")
    ensure_dir(processed_dir)

    # --- Lê consolidado do step1
    df = pd.read_csv(consolidated_csv, dtype=str, encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]

    required_cols = ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Consolidado não tem colunas obrigatórias: {missing}. Encontradas: {list(df.columns)}")

    # --- Normalizações fortes (corrige o que você viu: CNPJ curto e RazaoSocial numérica)
    df["CNPJ"] = df["CNPJ"].apply(normalize_cnpj)
    df["RazaoSocial"] = _normalize_razao_social(df["RazaoSocial"])

    # Trimestre/Ano/Valor como número (para validação), mas sem perder string original
    df["Trimestre"] = pd.to_numeric(df["Trimestre"], errors="coerce")
    df["Ano"] = pd.to_numeric(df["Ano"], errors="coerce")
    df["ValorDespesas"] = pd.to_numeric(df["ValorDespesas"], errors="coerce")

    # --- Validações mínimas
    # CNPJ DV
    df["CNPJ_VALIDO"] = df["CNPJ"].apply(is_valid_cnpj)

    # valor positivo
    df["VALOR_VALIDO"] = df["ValorDespesas"].notna() & (df["ValorDespesas"] > 0)

    # razão social não vazia
    df["RAZAO_VALIDO"] = df["RazaoSocial"].notna() & (df["RazaoSocial"].str.strip() != "")

    if strategy_invalid_cnpj == "drop":
        df = df[df["CNPJ_VALIDO"]].copy()
    elif strategy_invalid_cnpj == "keep_mark":
        # mantém tudo, mas marca para documentação/análise crítica
        pass
    else:
        raise ValueError("strategy_invalid_cnpj deve ser 'drop' ou 'keep_mark'")

    # Filtros básicos (CNPJ vazio, ano/trimestre nulos, valor inválido)
    df = df[df["CNPJ"].str.len() == 14].copy()
    df = df[df["Ano"].notna() & df["Trimestre"].notna()].copy()
    df = df[df["VALOR_VALIDO"]].copy()
    
    # Razão social vazia é OK - será preenchida no enriquecimento com cadastro ANS

    # --- Baixa e prepara cadastro
    cad_dl = download_latest_cadastro_csv(cadastro_dir_url)
    cad = _load_cadastro(cad_dl.csv_path)
    cad_best = _dedupe_cadastro(cad)

    # Salva o cadastro normalizado (baseado no arquivo ORIGINAL baixado)
    cadastro_normalizado_csv = processed_dir / "operadoras_ativas_normalizado.csv"
    cad_best.to_csv(cadastro_normalizado_csv, index=False, encoding="utf-8")

    # --- Enriquecimento (left join)
    enriched = df.merge(
        cad_best,
        how="left",
        on="CNPJ",
        suffixes=("", "_cad"),
    )

    # Renomeia para padrão do teste
    enriched = enriched.rename(
        columns={
            "RegistroANS": "RegistroANS",
            "Modalidade": "Modalidade",
            "UF": "UF",
        }
    )

    # Ordena e salva
    enriched_csv = processed_dir / "consolidado_enriquecido.csv"

    # Garantir tipos finais (CSV output)
    enriched["Ano"] = enriched["Ano"].astype(int)
    enriched["Trimestre"] = enriched["Trimestre"].astype(int)
    enriched["ValorDespesas"] = enriched["ValorDespesas"].astype(float)

    # Ordem final de colunas (mantém as pedidas + enrich)
    cols_out = [
        "CNPJ",
        "RazaoSocial",
        "Trimestre",
        "Ano",
        "ValorDespesas",
        "RegistroANS",
        "Modalidade",
        "UF",
    ]
    # Se keep_mark, mantém a marca de validade
    if strategy_invalid_cnpj == "keep_mark":
        cols_out.append("CNPJ_VALIDO")

    enriched.to_csv(enriched_csv, index=False, encoding="utf-8", columns=cols_out)

    out = Step2Outputs(
        enriched_csv=enriched_csv,
        aggregated_csv=None,  # agregado é gerado em outro módulo (aggregate.py)
        cadastro_csv=cadastro_normalizado_csv,
    )
    return enriched, out
