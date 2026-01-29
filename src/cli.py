from __future__ import annotations

import argparse

from src.pipeline.ans_fetch import run_step1_fetch_and_consolidate


def main() -> None:
    parser = argparse.ArgumentParser(description="IntuitiveCare Test CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("step1", help="Baixa e consolida últimos 3 trimestres (ANS)")
    s1.add_argument("--base-url", default="https://dadosabertos.ans.gov.br/FTP/PDA/", help="Base FTP PDA")
    s1.add_argument("--out", default="consolidado_despesas.csv", help="Nome do CSV final")

    args = parser.parse_args()

    if args.cmd == "step1":
        run_step1_fetch_and_consolidate(base_url=args.base_url, out_csv_name=args.out)


if __name__ == "__main__":
    main()
