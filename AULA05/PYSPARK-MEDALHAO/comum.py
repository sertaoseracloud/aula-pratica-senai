"""Configuracao compartilhada pelos tres jobs do laboratorio.

Tudo o que depende da maquina mora aqui: o interpretador dos workers, o
HADOOP_HOME do Windows e as opcoes da sessao. Os jobs de cada camada ficam
com o PySpark e nada mais.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"              # entrada bruta (CSV gerado pelo proprio job de Bronze)
CAMADAS = RAIZ / "camadas"          # as tres camadas da arquitetura medalhao
BRONZE = CAMADAS / "bronze"         # dado cru, so com proveniencia
SILVER = CAMADAS / "silver"         # dado limpo e validado
GOLD = CAMADAS / "gold"             # dado agregado, pronto para consumo


def preparar_ambiente() -> None:
    """Ajusta as variaveis que o Spark le antes de a JVM subir."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name == "nt":
        hadoop = RAIZ / "hadoop"
        if not (hadoop / "bin" / "winutils.exe").exists():
            raise SystemExit(
                "Falta hadoop/bin/winutils.exe. Rode o Passo 3 do SETUP.md."
            )
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["hadoop.home.dir"] = str(hadoop)
        os.environ["PATH"] = f"{hadoop / 'bin'}{os.pathsep}{os.environ['PATH']}"


def criar_sessao(nome: str):
    """Devolve uma SparkSession local ja configurada para o laboratorio."""
    preparar_ambiente()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(nome)
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
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")
