from __future__ import annotations

import argparse
from pathlib import Path

from src.pipeline.ans_fetch import run_step1_fetch_and_consolidate
from src.pipeline.validate_enrich import validate_and_enrich
from src.pipeline.aggregate import aggregate_operadora_uf, save_aggregated_csv
from src.pipeline.export_sql_data import export_sql_ready_files
from src.utils.fs import data_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="IntuitiveCare Test CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ---------- STEP 1 ----------
    s1 = sub.add_parser("step1", help="Baixa e consolida últimos 3 trimestres (ANS)")
    s1.add_argument(
        "--base-url",
        default="https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/",
        help="Diretório base das Demonstrações Contábeis (ANS)",
    )
    s1.add_argument(
        "--out",
        default="consolidado_despesas.csv",
        help="Nome do CSV final",
    )

    # ---------- STEP 2 ----------
    s2 = sub.add_parser(
        "step2",
        help="Valida, enriquece com cadastro e agrega por operadora/UF",
    )
    s2.add_argument(
        "--consolidated",
        default=str(data_dir("processed", "consolidado_despesas.csv")),
        help="CSV gerado no step1",
    )
    s2.add_argument(
        "--cadastro-url",
        default="https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/",
        help="Diretório do cadastro de operadoras ativas",
    )
    s2.add_argument(
        "--invalid-cnpj-strategy",
        choices=["drop", "keep_mark"],
        default="drop",
        help="Estratégia para CNPJ inválido",
    )

    # ---------- EXPORT SQL ----------
    sub.add_parser(
        "export-sql",
        help="Gera CSVs prontos para carga no PostgreSQL (sql_data/)",
    )

    args = parser.parse_args()

    if args.cmd == "step1":
        run_step1_fetch_and_consolidate(
            base_url=args.base_url,
            out_csv_name=args.out,
        )

    elif args.cmd == "step2":
        consolidated_path = Path(args.consolidated)
        enriched_df, out = validate_and_enrich(
            consolidated_csv=consolidated_path,
            cadastro_dir_url=args.cadastro_url,
            strategy_invalid_cnpj=args.invalid_cnpj_strategy,
        )
        agg = aggregate_operadora_uf(enriched_df)
        out_csv = save_aggregated_csv(agg)
        print(f"[OK] Consolidado enriquecido: {out.enriched_csv}")
        print(f"[OK] Agregado: {out_csv}")

    elif args.cmd == "export-sql":
        out_dir = export_sql_ready_files()
        print(f"[OK] Arquivos SQL-ready gerados em: {out_dir}")


if __name__ == "__main__":
    main()
