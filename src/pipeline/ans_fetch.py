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
# Discovery (YYYY/QQ/) robusto
# ---------------------------

YEAR_RE = re.compile(r"^(19|20)\d{2}$")
QUARTER_RE = re.compile(r"^(?:Q([1-4])|([1-4])T|([1-4])|0?([1-4]))$", re.IGNORECASE)


@dataclass(frozen=True)
class QuarterFolder:
    year: int
    quarter: int
    url: str  # ends with /


def _extract_links(html: str) -> list[str]:
    # pega href de forma simples (listagens do Apache/NGINX geralmente funcionam assim)
    return re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)


def _is_dir_link(href: str) -> bool:
    return href.endswith("/") and not href.startswith("?") and href not in ("../", "./")


def _normalize_quarter(name: str) -> int | None:
    n = name.strip().strip("/")
    m = QUARTER_RE.match(n)
    if not m:
        return None
    for g in m.groups():
        if g:
            return int(g)
    return None


def _find_quarter_folders(base_url: str, max_depth: int = 2) -> list[QuarterFolder]:
    """
    Busca por estrutura YYYY/QQ/ a partir do base_url, com BFS até max_depth.
    Motivo: o enunciado avisa que alguns trimestres podem ter estruturas diferentes. :contentReference[oaicite:5]{index=5}
    """
    # BFS por diretórios
    queue: list[tuple[str, int]] = [(base_url if base_url.endswith("/") else base_url + "/", 0)]
    seen: set[str] = set()

    found: list[QuarterFolder] = []

    while queue:
        url, depth = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)

        html = get_text(url)
        links = _extract_links(html)

        # procura anos no nível atual
        year_dirs = [h for h in links if _is_dir_link(h) and YEAR_RE.match(h.strip("/"))]
        if year_dirs:
            for y in year_dirs:
                year = int(y.strip("/"))
                year_url = urljoin(url, y)
                yhtml = get_text(year_url)
                qlinks = _extract_links(yhtml)
                quarter_dirs = [h for h in qlinks if _is_dir_link(h)]
                for q in quarter_dirs:
                    qn = _normalize_quarter(q)
                    if qn and 1 <= qn <= 4:
                        found.append(QuarterFolder(year=year, quarter=qn, url=urljoin(year_url, q)))
            continue  # já achou a estrutura aqui, não precisa descer mais

        # se não achou, desce mais 1 nível (até max_depth)
        if depth < max_depth:
            dir_links = [h for h in links if _is_dir_link(h)]
            for h in dir_links:
                child = urljoin(url, h)
                queue.append((child, depth + 1))

    # remove duplicados por (year,quarter,url)
    uniq = {(q.year, q.quarter, q.url): q for q in found}
    return list(uniq.values())


def _pick_latest_3(quarters: list[QuarterFolder]) -> list[QuarterFolder]:
    quarters_sorted = sorted(quarters, key=lambda q: (q.year, q.quarter), reverse=True)
    return quarters_sorted[:3]


# ---------------------------
# Download + extract
# ---------------------------

def _list_zip_links(folder_url: str) -> list[str]:
    html = get_text(folder_url)
    links = _extract_links(html)
    zips = [h for h in links if h.lower().endswith(".zip")]
    # alguns servers trazem links relativos; normaliza para URL completa
    return [urljoin(folder_url, z) for z in zips]


def _download_and_extract(zip_urls: list[str], out_dir: Path) -> list[Path]:
    ensure_dir(out_dir)
    extracted_files: list[Path] = []

    for zurl in zip_urls:
        fname = safe_filename(zurl.split("/")[-1])
        zip_path = out_dir / fname
        if not zip_path.exists():
            download_file(zurl, str(zip_path))

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(out_dir / zip_path.stem)

        for p in (out_dir / zip_path.stem).rglob("*"):
            if p.is_file():
                extracted_files.append(p)

    return extracted_files


# ---------------------------
# Parse + normalize (CSV/TXT/XLSX)
# ---------------------------

KEYWORDS = ("despesa", "despesas", "sinistro", "sinistros", "evento", "eventos")


def _looks_like_expense_file(path: Path) -> bool:
    n = path.name.lower()
    return any(k in n for k in KEYWORDS)


def _read_any_table(path: Path) -> pd.DataFrame | None:
    suffix = path.suffix.lower()

    try:
        if suffix in (".csv", ".txt"):
            # tenta separadores comuns
            for sep in (";", ",", "\t", "|"):
                try:
                    df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8", engine="python")
                    if df.shape[1] >= 3:
                        return df
                except Exception:
                    continue
            return None

        if suffix in (".xlsx", ".xls"):
            df = pd.read_excel(path, dtype=str)
            return df

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
    # remove moedas e espaços
    v = re.sub(r"[R$\s]", "", v, flags=re.IGNORECASE)
    # formatos comuns: "1.234,56" ou "1234,56" ou "1234.56"
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    elif "," in v and "." not in v:
        v = v.replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None


def _normalize_table(df: pd.DataFrame, year: int, quarter: int) -> pd.DataFrame | None:
    # heurísticas simples e explicáveis (KISS) para detectar colunas
    cnpj_col = _find_col(df, ["cnpj"])
    razao_col = _find_col(df, ["razaosocial", "razao", "nome"])
    # para valor, tenta achar "valor"+"desp/sinistro/evento" ou só "valor"
    valor_col = _find_col(df, ["valordesp", "valor_desp", "valor", "vlr", "vld"])

    if not cnpj_col or not razao_col or not valor_col:
        return None

    out = pd.DataFrame()
    out["CNPJ"] = df[cnpj_col].astype(str).str.replace(r"\D", "", regex=True)
    out["RazaoSocial"] = df[razao_col].astype(str).str.strip()
    out["Ano"] = year
    out["Trimestre"] = quarter

    values = df[valor_col].astype(str).apply(_to_decimal_br)
    out["ValorDespesas"] = values

    # remove linhas totalmente vazias
    out = out.dropna(subset=["CNPJ", "RazaoSocial", "ValorDespesas"], how="any")
    out = out[out["CNPJ"].str.len() == 14]
    return out


# ---------------------------
# Orquestração do Step 1
# ---------------------------

def run_step1_fetch_and_consolidate(base_url: str, out_csv_name: str) -> Path:
    raw_dir = data_dir("raw")
    processed_dir = data_dir("processed")
    ensure_dir(raw_dir)
    ensure_dir(processed_dir)

    quarters = _find_quarter_folders(base_url=base_url, max_depth=2)
    if not quarters:
        raise RuntimeError(
            "Não encontrei pastas no padrão YYYY/QQ/ a partir do base_url. "
            "Tente apontar base_url para o diretório de Demonstrações Contábeis."
        )

    last3 = _pick_latest_3(quarters)

    all_frames: list[pd.DataFrame] = []
    suspect_rows: list[pd.DataFrame] = []

    for q in last3:
        zip_urls = _list_zip_links(q.url)
        if not zip_urls:
            continue

        qdir = raw_dir / f"{q.year}_T{q.quarter}"
        extracted = _download_and_extract(zip_urls, qdir)

        candidates = [p for p in extracted if _looks_like_expense_file(p)]
        for p in candidates:
            df = _read_any_table(p)
            if df is None or df.empty:
                continue
            norm = _normalize_table(df, year=q.year, quarter=q.quarter)
            if norm is None or norm.empty:
                continue

            # inconsistências (registrar)
            neg_or_zero = norm[norm["ValorDespesas"] <= 0]
            if not neg_or_zero.empty:
                suspect_rows.append(neg_or_zero.assign(Motivo="valor_zero_ou_negativo", FonteArquivo=str(p)))

            # estratégia KISS para Step 1: manter tudo aqui e deixar validação “oficial” para Step 2
            all_frames.append(norm.assign(FonteArquivo=str(p)))

    if not all_frames:
        raise RuntimeError("Não consegui extrair nenhum dado válido de despesas eventos/sinistros.")

    consolidated = pd.concat(all_frames, ignore_index=True)

    # estratégia simples para CNPJ duplicado com razão social diferente:
    # mantém a razão social mais frequente por CNPJ e marca divergências em suspeitos.
    grp = consolidated.groupby("CNPJ")["RazaoSocial"]
    mode_rs = grp.agg(lambda s: s.value_counts().index[0])
    consolidated["RazaoSocialModo"] = consolidated["CNPJ"].map(mode_rs)

    diverge = consolidated[consolidated["RazaoSocial"] != consolidated["RazaoSocialModo"]]
    if not diverge.empty:
        suspect_rows.append(diverge.assign(Motivo="cnpj_com_razao_social_divergente"))

    consolidated["RazaoSocial"] = consolidated["RazaoSocialModo"]
    consolidated = consolidated.drop(columns=["RazaoSocialModo"])

    out_csv = processed_dir / out_csv_name
    consolidated[["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]].to_csv(out_csv, index=False, encoding="utf-8")

    # salva suspeitos para transparência (você cita isso no README)
    if suspect_rows:
        sus = pd.concat(suspect_rows, ignore_index=True)
        (processed_dir / "suspeitos_step1.csv").write_text(
            sus.to_csv(index=False, encoding="utf-8"),
            encoding="utf-8"
        )

    return out_csv
