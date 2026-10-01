"""Executa as camadas do pipeline supervisionado em processos separados."""

import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
JOBS = (
    "01_bronze_ingestao.py",
    "02_silver_preparacao.py",
    "03_gold_supervisionado.py",
)


def main() -> None:
    for job in JOBS:
        print(f"\n{'#' * 68}\n# {job}\n{'#' * 68}", flush=True)
        resultado = subprocess.run(
            [sys.executable, str(RAIZ / job)], check=False
        )
        if resultado.returncode:
            raise SystemExit(f"{job} falhou; pipeline interrompido.")


if __name__ == "__main__":
    main()
