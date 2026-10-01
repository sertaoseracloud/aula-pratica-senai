"""Verificacoes locais do interpretador, PySpark, Spark ML e Java."""

import os
import shutil
import subprocess
import sys


def main() -> None:
    print(f"Python: {sys.version.split()[0]} ({sys.executable})")
    if not (3, 8) <= sys.version_info[:2] <= (3, 12):
        raise SystemExit("Use Python 3.8 a 3.12; 3.11 e recomendado.")

    try:
        import pyspark
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.regression import RandomForestRegressor
    except ImportError as erro:
        raise SystemExit(f"PySpark/Spark ML nao disponivel: {erro}") from erro
    modelos = (LogisticRegression, RandomForestRegressor, KMeans)
    nomes_modelos = ", ".join(modelo.__name__ for modelo in modelos)
    print(f"PySpark: {pyspark.__version__}; Spark ML: {nomes_modelos}")

    java = shutil.which("java")
    if java is None:
        raise SystemExit(
            "Java nao encontrado no PATH; instale JDK/JRE 8, 11 ou 17."
        )
    versao = subprocess.run([java, "-version"], capture_output=True, text=True)
    print((versao.stderr or versao.stdout).splitlines()[0])

    if os.name == "nt":
        hadoop = os.path.join(os.path.dirname(__file__), "hadoop", "bin")
        if not os.path.isfile(os.path.join(hadoop, "winutils.exe")):
            raise SystemExit(
                "Falta hadoop/bin/winutils.exe; veja o Passo 4 do SETUP.md."
            )
    print("Ambiente pronto. Comece por: python 01_bronze_ingestao.py")


if __name__ == "__main__":
    main()
