"""Roda os tres jobs da arquitetura medalhao em sequencia.

    python executar_pipeline.py
    python executar_pipeline.py --limpar   # apaga as camadas antes de comecar

Isto NAO e um orquestrador de verdade -- e um atalho de conveniencia para
quem quer rodar o pipeline inteiro sem digitar tres comandos. Um Airflow,
um Dagster ou um Step Functions fariam este mesmo papel em produção: cada
job continua sendo um programa independente, que roda sozinho, le a camada
anterior do disco e nao sabe nada sobre quem o chamou.

Cada job sobe e derruba a sua PROPRIA SparkSession -- por isso este script
chama cada um como um processo separado (`subprocess.run`), em vez de
importar as funcoes e rodar tudo numa sessao so. E assim que jobs
encadeados rodam de verdade: um span de vida de JVM por job, nao uma
sessao compartilhada acumulando estado entre etapas que deveriam ser
independentes.
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

    print("\nPipeline completo: Bronze -> Silver -> Gold.")


if __name__ == "__main__":
    main()
