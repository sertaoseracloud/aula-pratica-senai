"""Confere o ambiente requerido pela trilha nao supervisionada."""

import os
import shutil
import sys


def main() -> None:
    if not (3, 8) <= sys.version_info[:2] <= (3, 12):
        raise SystemExit("Use Python 3.8 a 3.12; 3.11 e recomendado.")
    try:
        import pyspark
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import PCA
    except ImportError as erro:
        raise SystemExit(f"PySpark nao disponivel: {erro}") from erro
    print(
        f"Python {sys.version.split()[0]} | PySpark {pyspark.__version__} | "
        f"{KMeans.__name__} + {PCA.__name__}"
    )
    if os.name == "nt":
        hadoop = os.path.join(os.path.dirname(__file__), "hadoop", "bin")
        if not os.path.isfile(os.path.join(hadoop, "winutils.exe")):
            raise SystemExit("Falta hadoop/bin/winutils.exe. Veja SETUP.md.")
    if shutil.which("java") is None:
        raise SystemExit("Java 8, 11 ou 17 nao encontrado no PATH.")
    print("Ambiente pronto.")


if __name__ == "__main__":
    main()
