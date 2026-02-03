from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd

from src.utils.fs import data_dir, ensure_dir, safe_filename
from src.utils.http import get_text, download_file


# ---------------------------
# Modelos / regex
# ---------------------------

YEAR_RE = re.compile(r"^(19|20)\d{2}/?$")
ZIP_TRIMESTRE_RE = re.compile(r"^([1-4])T(20\d{2})\.zip$", re.IGNORECASE)
KEYWORDS = ("despesa", "despesas", "sinistro", "sinistros", "evento", "eventos", "t20", "t2025", "1t", "2t", "3t", "4t")


@dataclass(frozen=True)
class QuarterZip:
    year: int
    quarter: int
    url: str  # link completo pro zip


# ---------------------------
# HTML parsing simples (listagem de diretório)
# ---------------------------

def _extract_links(html: str) -> list[str]:
    return re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)


def _is_dir_link(href: str) -> bool:
    return href.endswith("/") and href not in ("../", "./") and not href.startswith("?")


# ---------------------------
# Discovery: encontra zips trimestrais no padrão real
# ---------------------------

def _list_year_dirs(base_url: str) -> list[str]:
    """Lista diretórios de ano (ex.: 2025/) dentro de demonstracoes_contabeis/."""
    base_url = base_url if base_url.endswith("/") else base_url + "/"
    html = get_text(base_url)
    links = _extract_links(html)
    years = [h for h in links if _is_dir_link(h) and YEAR_RE.match(h.strip("/"))]
    # normaliza com / no fim
    years = [y if y.endswith("/") else y + "/" for y in years]
    return years


def _list_quarter_zips_for_year(year_url: str) -> list[QuarterZip]:
    """Dentro de YYYY/, encontra arquivos tipo 1T2025.zip."""
    html = get_text(year_url)
    links = _extract_links(html)
    zips = []
    for h in links:
        name = h.split("/")[-1]
        m = ZIP_TRIMESTRE_RE.match(name)
        if not m:
            continue
        q = int(m.group(1))
        y = int(m.group(2))
        zips.append(QuarterZip(year=y, quarter=q, url=urljoin(year_url, name)))
    return zips


def _find_all_quarter_zips(base_url: str) -> list[QuarterZip]:
    """Varre todos os anos e retorna todos os ZIPs trimestrais encontrados."""
    base_url = base_url if base_url.endswith("/") else base_url + "/"
    year_dirs = _list_year_dirs(base_url)
    all_qz: list[QuarterZip] = []

    for yd in year_dirs:
        year_url = urljoin(base_url, yd)
        all_qz.extend(_list_quarter_zips_for_year(year_url))

    # remove duplicados (por url)
    uniq = {qz.url: qz for qz in all_qz}
    return list(uniq.values())


def _pick_latest_3(qz_list: list[QuarterZip]) -> list[QuarterZip]:
    """Escolhe os 3 trimestres mais recentes considerando (year, quarter)."""
    return sorted(qz_list, key=lambda x: (x.year, x.quarter), reverse=True)[:3]


# ---------------------------
# Download + extract
# ---------------------------

def _download_and_extract_zip(zip_url: str, out_dir: Path) -> list[Path]:
    ensure_dir(out_dir)
    fname = safe_filename(zip_url.split("/")[-1])
    zip_path = out_dir / fname

    if not zip_path.exists():
        download_file(zip_url, str(zip_path))

    extract_dir = out_dir / zip_path.stem
    ensure_dir(extract_dir)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    files = [p for p in extract_dir.rglob("*") if p.is_file()]
    return files


# ---------------------------
# Parse + normalize (CSV/TXT/XLSX)
# ---------------------------

def _looks_like_expense_file(path: Path) -> bool:
    n = path.name.lower()
    return any(k in n for k in KEYWORDS)


def _read_any_table(path: Path) -> pd.DataFrame | None:
    suffix = path.suffix.lower()
    try:
        if suffix in (".csv", ".txt"):
            for sep in (";", ",", "\t", "|"):
                try:
                    df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8", engine="python")
                    if df.shape[1] >= 3:
                        return df
                except Exception:
                    continue
            return None

        if suffix in (".xlsx", ".xls"):
            return pd.read_excel(path, dtype=str)

        return None
    except Exception:
        return None


def _find_col(df: pd.DataFrame, patterns: list[str]) -> str | None:
    cols = list(df.columns)
    norm = {c: re.sub(r"\s+", "", str(c)).lower() for c in cols}
    for c in cols:
        name = norm[c]
        if any(p in name for p in patterns):
            return c
    return None


def _to_decimal_br(value: str) -> float | None:
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


def _normalize_table(df: pd.DataFrame, year: int, quarter: int) -> pd.DataFrame | None:
    cnpj_col = _find_col(df, ["cnpj"])
    razao_col = _find_col(df, ["razaosocial", "razao", "nome"])
    valor_col = _find_col(df, ["valor", "vlr", "vld", "desp"])

    if cnpj_col and razao_col and valor_col:
        out = pd.DataFrame()
        out["CNPJ"] = df[cnpj_col].astype(str).str.replace(r"\D", "", regex=True)
        out["RazaoSocial"] = df[razao_col].astype(str).str.strip()
        out["Ano"] = year
        out["Trimestre"] = quarter
        out["ValorDespesas"] = df[valor_col].astype(str).apply(_to_decimal_br)

        out = out.dropna(subset=["CNPJ", "RazaoSocial", "ValorDespesas"], how="any")
        out = out[out["CNPJ"].str.len() == 14]
        out = out[out["ValorDespesas"] > 0]
        return out

    reg_ans_col = _find_col(df, ["regans", "reg_ans", "operadora"])
    saldo_col = _find_col(df, ["vl_saldo_final", "vlsaldofinal", "saldofinal", "valor"])
    
    if reg_ans_col and saldo_col:
        out = pd.DataFrame()
        out["CNPJ"] = df[reg_ans_col].astype(str).str.replace(r"\D", "", regex=True)
        out["RazaoSocial"] = df[reg_ans_col].astype(str).str.strip()
        out["Ano"] = year
        out["Trimestre"] = quarter
        out["ValorDespesas"] = df[saldo_col].astype(str).apply(_to_decimal_br)

        out = out.dropna(subset=["CNPJ", "ValorDespesas"], how="any")
        out = out[out["ValorDespesas"] > 0]
        return out

    return None


# ---------------------------
# Orquestração do Step 1
# ---------------------------

def run_step1_fetch_and_consolidate(base_url: str, out_csv_name: str) -> Path:
    raw_dir = data_dir("raw")
    processed_dir = data_dir("processed")
    ensure_dir(raw_dir)
    ensure_dir(processed_dir)

    qz_all = _find_all_quarter_zips(base_url)
    if not qz_all:
        raise RuntimeError(
            "Não encontrei ZIPs trimestrais no padrão 1TYYYY.zip. "
            "Verifique a URL base (ex.: .../demonstracoes_contabeis/)."
        )

    last3 = _pick_latest_3(qz_all)

    all_frames: list[pd.DataFrame] = []
    suspect_rows: list[pd.DataFrame] = []

    for qz in last3:
        qdir = raw_dir / f"{qz.year}_T{qz.quarter}"
        extracted_files = _download_and_extract_zip(qz.url, qdir)

        # candidatos por nome (KISS)
        candidates = [p for p in extracted_files if _looks_like_expense_file(p)]
        for p in candidates:
            df = _read_any_table(p)
            if df is None or df.empty:
                continue
            norm = _normalize_table(df, year=qz.year, quarter=qz.quarter)
            if norm is None or norm.empty:
                continue
            all_frames.append(norm.assign(FonteArquivo=str(p)))

    if not all_frames:
        raise RuntimeError(
            "Não consegui extrair dados de despesas/sinistros dos ZIPs. "
            "Pode ser que os arquivos tenham nomes diferentes do esperado; "
            "nesse caso ajustamos o filtro KEYWORDS."
        )

    consolidated = pd.concat(all_frames, ignore_index=True)

    # resolve divergência de razão social por CNPJ (moda) + registra suspeitos
    mode_rs = consolidated.groupby("CNPJ")["RazaoSocial"].agg(lambda s: s.value_counts().index[0])
    consolidated["RazaoSocialModo"] = consolidated["CNPJ"].map(mode_rs)

    diverge = consolidated[consolidated["RazaoSocial"] != consolidated["RazaoSocialModo"]]
    if not diverge.empty:
        suspect_rows.append(diverge.assign(Motivo="cnpj_com_razao_social_divergente"))

    consolidated["RazaoSocial"] = consolidated["RazaoSocialModo"]
    consolidated = consolidated.drop(columns=["RazaoSocialModo"])

    out_csv = processed_dir / out_csv_name
    consolidated[["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]].to_csv(
        out_csv, index=False, encoding="utf-8"
    )

    if suspect_rows:
        sus = pd.concat(suspect_rows, ignore_index=True)
        sus.to_csv(processed_dir / "suspeitos_step1.csv", index=False, encoding="utf-8")

    return out_csv

    # --- obrigatório do enunciado: compactar consolidado em ZIP ---
    out_zip = processed_dir / "consolidado_despesas.zip"
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Nome interno do arquivo no zip (limpo e previsível)
        zf.write(out_csv, arcname=out_csv.name)

