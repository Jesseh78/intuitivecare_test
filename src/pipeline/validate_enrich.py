from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd

from src.utils.fs import data_dir, ensure_dir, safe_filename
from src.utils.http import get_text, download_file


# ---------------------------
# Regras de validação
# ---------------------------

def only_digits(value: str) -> str:
    """Remove tudo que não for dígito."""
    return re.sub(r"\D", "", str(value or ""))


def is_cnpj_valid(cnpj: str) -> bool:
    """
    Valida CNPJ (14 dígitos) com dígitos verificadores.
    Regras:
      - Deve ter 14 dígitos
      - Não pode ser sequência de dígitos iguais (ex: 000.., 111..)
      - Calcula DV1 e DV2
    """
    cnpj = only_digits(cnpj)
    if len(cnpj) != 14:
        return False
    if cnpj == cnpj[0] * 14:
        return False

    def calc_dv(nums: list[int], weights: list[int]) -> int:
        s = sum(n * w for n, w in zip(nums, weights))
        r = s % 11
        return 0 if r < 2 else 11 - r

    nums = [int(c) for c in cnpj]
    base = nums[:12]
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    dv1 = calc_dv(base, w1)
    dv2 = calc_dv(base + [dv1], w2)
    return nums[12] == dv1 and nums[13] == dv2


def to_decimal_br(value: str) -> float | None:
    """
    Converte número em formatos comuns (BR e US) para float.
    Exemplos:
      "1.234,56" -> 1234.56
      "1234,56"  -> 1234.56
      "1234.56"  -> 1234.56
    """
    if value is None:
        return None
    v = str(value).strip()
    if v == "":
        return None
    v = re.sub(r"[R$\s]", "", v, flags=re.IGNORECASE)
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    elif "," in v and "." not in v:
        v = v.replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None


# ---------------------------
# Download do cadastro ANS
# ---------------------------

@dataclass(frozen=True)
class CadastroDownloadResult:
    csv_path: Path
    source_url: str


def _extract_links(html: str) -> list[str]:
    return re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)


def download_operadoras_ativas_csv(base_dir_url: str) -> CadastroDownloadResult:
    """
    Baixa o arquivo CSV de 'Dados Cadastrais das Operadoras Ativas' do diretório ANS.

    Estratégia (robusta e simples):
      - abre o HTML do diretório
      - procura links .csv (ou .zip com .csv dentro não está no enunciado, mas deixamos só .csv)
      - escolhe o maior nome (tende a ser o mais completo/atual em listagens)
    """
    base_dir_url = base_dir_url if base_dir_url.endswith("/") else base_dir_url + "/"
    html = get_text(base_dir_url)
    links = _extract_links(html)

    csv_links = [h for h in links if h.lower().endswith(".csv")]
    if not csv_links:
        raise RuntimeError("Não encontrei nenhum .csv no diretório de operadoras ativas.")

    # escolhe “melhor candidato”: nome mais longo (tende a incluir sufixos/versões)
    chosen = sorted(csv_links, key=lambda s: (len(s), s), reverse=True)[0]
    csv_url = urljoin(base_dir_url, chosen)

    ref_dir = data_dir("reference")
    ensure_dir(ref_dir)

    out_name = safe_filename(chosen.split("/")[-1])
    out_path = ref_dir / out_name

    if not out_path.exists():
        download_file(csv_url, str(out_path))

    return CadastroDownloadResult(csv_path=out_path, source_url=csv_url)


# ---------------------------
# Validação + join
# ---------------------------

@dataclass(frozen=True)
class Step2Outputs:
    enriched_csv: Path
    aggregated_csv: Path
    invalid_rows_csv: Path
    join_report_csv: Path
    cadastro_csv: Path


def validate_and_enrich(
    consolidated_csv: Path,
    cadastro_dir_url: str,
    strategy_invalid_cnpj: str = "drop",
) -> tuple[pd.DataFrame, Step2Outputs]:
    """
    Lê o consolidado (Step 1), valida e enriquece com cadastro.

    strategy_invalid_cnpj:
      - "drop": remove registros com CNPJ inválido do dataset final (recomendado)
      - "keep_mark": mantém, mas marca como inválido (não recomendado para análises/SQL)
    """
    processed_dir = data_dir("processed")
    ensure_dir(processed_dir)

    # 1) Lê consolidado
    df = pd.read_csv(consolidated_csv, dtype=str, encoding="utf-8")
    # normaliza colunas (garante tipos)
    df["CNPJ"] = df["CNPJ"].astype(str).apply(only_digits)
    df["RazaoSocial"] = df["RazaoSocial"].astype(str).str.strip()
    df["Ano"] = df["Ano"].astype(str)
    df["Trimestre"] = df["Trimestre"].astype(str)

    # ValorDespesas vira float (para agregação e SQL)
    df["ValorDespesas"] = df["ValorDespesas"].apply(to_decimal_br)

    # 2) Aplica validações
    invalid_reasons: list[str] = []

    cnpj_ok = df["CNPJ"].apply(is_cnpj_valid)
    razao_ok = df["RazaoSocial"].ne("")
    valor_ok = df["ValorDespesas"].notna() & (df["ValorDespesas"] > 0)

    df["__cnpj_ok"] = cnpj_ok
    df["__razao_ok"] = razao_ok
    df["__valor_ok"] = valor_ok

    invalid_mask = ~(cnpj_ok & razao_ok & valor_ok)

    invalid_df = df.loc[invalid_mask].copy()
    if not invalid_df.empty:
        def reason_row(r) -> str:
            reasons = []
            if not r["__cnpj_ok"]:
                reasons.append("cnpj_invalido")
            if not r["__razao_ok"]:
                reasons.append("razao_social_vazia")
            if not r["__valor_ok"]:
                reasons.append("valor_invalido_ou_nao_positivo")
            return "|".join(reasons)

        invalid_df["MotivoInvalidacao"] = invalid_df.apply(reason_row, axis=1)

    invalid_rows_csv = processed_dir / "invalid_rows_step2.csv"
    invalid_df.drop(columns=["__cnpj_ok", "__razao_ok", "__valor_ok"], errors="ignore").to_csv(
        invalid_rows_csv, index=False, encoding="utf-8"
    )

    if strategy_invalid_cnpj == "drop":
        df = df.loc[~invalid_mask].copy()
    elif strategy_invalid_cnpj == "keep_mark":
        df["IsValid"] = ~invalid_mask
    else:
        raise ValueError("strategy_invalid_cnpj deve ser 'drop' ou 'keep_mark'.")

    # remove colunas internas
    df = df.drop(columns=["__cnpj_ok", "__razao_ok", "__valor_ok"], errors="ignore")

    # 3) Baixa cadastro
    cad = download_operadoras_ativas_csv(cadastro_dir_url)
    cad_df = pd.read_csv(cad.csv_path, dtype=str, encoding="utf-8", sep=None, engine="python")
    # Normaliza CNPJ
    # (no cadastro, algumas versões trazem "CNPJ" ou "CNPJ da Operadora", então tentamos achar)
    cad_cols_norm = {c: re.sub(r"\s+", "", str(c)).lower() for c in cad_df.columns}
    cnpj_col = None
    for c, n in cad_cols_norm.items():
        if "cnpj" in n:
            cnpj_col = c
            break
    if not cnpj_col:
        raise RuntimeError("Não encontrei coluna de CNPJ no cadastro de operadoras ativas.")

    # acha colunas de interesse
    def find_col_contains(key: str) -> str | None:
        for c, n in cad_cols_norm.items():
            if key in n:
                return c
        return None

    registro_col = find_col_contains("registroans") or find_col_contains("registro")
    modalidade_col = find_col_contains("modalidade")
    uf_col = find_col_contains("uf")

    if not registro_col or not modalidade_col or not uf_col:
        raise RuntimeError("Cadastro não contém (RegistroANS, Modalidade, UF) de forma detectável.")

    cad_df["CNPJ"] = cad_df[cnpj_col].astype(str).apply(only_digits)
    cad_df["RegistroANS"] = cad_df[registro_col].astype(str).str.strip()
    cad_df["Modalidade"] = cad_df[modalidade_col].astype(str).str.strip()
    cad_df["UF"] = cad_df[uf_col].astype(str).str.strip()

    cad_df = cad_df[["CNPJ", "RegistroANS", "Modalidade", "UF"]].copy()

    # 4) Trata duplicidade no cadastro (CNPJ repetido com dados diferentes)
    # Estratégia escolhida:
    #  - para cada CNPJ, escolhe a linha com maior "completude" (mais campos preenchidos)
    #  - em empate, menor RegistroANS (determinístico)
    cad_df["__filled"] = (
        cad_df["RegistroANS"].ne("").astype(int)
        + cad_df["Modalidade"].ne("").astype(int)
        + cad_df["UF"].ne("").astype(int)
    )
    cad_df_sorted = cad_df.sort_values(
        by=["CNPJ", "__filled", "RegistroANS"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    cad_best = cad_df_sorted.drop_duplicates(subset=["CNPJ"], keep="first").drop(columns=["__filled"])

    # relatório de duplicados (para mostrar “análise crítica”)
    dup_mask = cad_df.duplicated(subset=["CNPJ"], keep=False)
    dup_report = cad_df.loc[dup_mask].sort_values(["CNPJ", "RegistroANS"], kind="mergesort").copy()
    dup_report_csv = processed_dir / "cadastro_duplicados_step2.csv"
    dup_report.to_csv(dup_report_csv, index=False, encoding="utf-8")

    # 5) Join
    enriched = df.merge(cad_best, on="CNPJ", how="left")

    # relatório de join sem match
    join_report = enriched[enriched["RegistroANS"].isna() | enriched["UF"].isna()].copy()
    join_report["MotivoJoin"] = "sem_match_no_cadastro"
    join_report_csv = processed_dir / "join_sem_match_step2.csv"
    join_report.to_csv(join_report_csv, index=False, encoding="utf-8")

    # salva consolidado enriquecido
    enriched_csv = processed_dir / "consolidado_enriquecido.csv"
    enriched.to_csv(enriched_csv, index=False, encoding="utf-8")

    # (agregação é feita no outro módulo)
    # placeholder de paths, preenchidos pelo chamador
    out = Step2Outputs(
        enriched_csv=enriched_csv,
        aggregated_csv=processed_dir / "despesas_agregadas.csv",
        invalid_rows_csv=invalid_rows_csv,
        join_report_csv=join_report_csv,
        cadastro_csv=cad.csv_path,
    )
    return enriched, out
