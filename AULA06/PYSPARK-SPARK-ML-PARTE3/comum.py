"""Configuracao compartilhada dos jobs Spark ML, parte 3."""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"
CAMADAS = RAIZ / "camadas"
BRONZE = CAMADAS / "bronze"
SILVER = CAMADAS / "silver"
GOLD = CAMADAS / "gold"


def preparar_ambiente() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name == "nt":
        hadoop = RAIZ / "hadoop"
        if not (hadoop / "bin" / "winutils.exe").exists():
            raise SystemExit("Falta hadoop/bin/winutils.exe. Veja o SETUP.md.")
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["hadoop.home.dir"] = str(hadoop)
        os.environ["PATH"] = (
            f"{hadoop / 'bin'}{os.pathsep}{os.environ['PATH']}"
        )


def criar_sessao(nome: str):
    preparar_ambiente()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(nome)
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def titulo(texto: str) -> None:
    print(f"\n{'=' * 72}\n{texto}\n{'=' * 72}")
