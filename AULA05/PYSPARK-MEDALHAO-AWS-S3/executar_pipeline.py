"""Roda os tres jobs da arquitetura medalhao (com AWS S3) em sequencia.

    python executar_pipeline.py
    python executar_pipeline.py --limpar   # apaga a copia local das camadas antes

Atalho de conveniencia, nao um orquestrador de verdade -- veja a docstring
do `executar_pipeline.py` do laboratorio 100% local (AULA05/PYSPARK-MEDALHAO)
para a explicacao completa de por que cada job roda como processo separado.
"""

import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
PYTHON = sys.executable

JOBS = [
    "01_bronze_ingestao.py",
    "02_silver_limpeza.py",
    "03_gold_agregados.py",
]


def main() -> None:
    if "--limpar" in sys.argv:
        subprocess.run([PYTHON, str(RAIZ / JOBS[0]), "--limpar"], check=True)

    for job in JOBS:
        print(f"\n{'#' * 70}\n# {job}\n{'#' * 70}")
        resultado = subprocess.run([PYTHON, str(RAIZ / job)])
        if resultado.returncode != 0:
            raise SystemExit(
                f"\n{job} falhou (codigo {resultado.returncode}) -- "
                "pipeline interrompido. Corrija e rode de novo a partir daqui, "
                "ou rode so este job com `python " + job + "`."
            )

    print("\nPipeline completo: Bronze -> Silver -> Gold, tudo no S3.")


if __name__ == "__main__":
    main()
