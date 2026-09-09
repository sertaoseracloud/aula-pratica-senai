"""Configuracao compartilhada pelos tres scripts do laboratorio.

Tudo o que depende da maquina mora aqui: o interpretador dos workers, o
HADOOP_HOME do Windows e as opcoes da sessao. Os scripts de exemplo ficam
com o PySpark e nada mais.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"          # entrada bruta (CSV gerado pelo proprio lab)
SAIDA = RAIZ / "saida"          # destino do ETL (Parquet particionado)


def preparar_ambiente() -> None:
    """Ajusta as variaveis que o Spark le antes de a JVM subir.

    Chamar isto DEPOIS de importar pyspark nao adianta em parte dos casos:
    a JVM le o ambiente no momento em que o gateway sobe. Por isso a funcao
    e chamada no topo de cada script, antes de criar a sessao.
    """
    # Sem isto o Spark lanca os workers com o `python` do PATH -- que pode ser
    # outra versao. O sintoma ("Python worker failed to connect back") nao diz
    # nada sobre versao de Python.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # No Windows o Hadoop precisa de winutils.exe/hadoop.dll ate para gravar
    # em disco local. Sem isso a sessao nem chega a subir:
    #   java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
    if os.name == "nt":
        hadoop = RAIZ / "hadoop"
        if not (hadoop / "bin" / "winutils.exe").exists():
            raise SystemExit(
                "Falta hadoop/bin/winutils.exe. Rode o Passo 2 do README."
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
        # local[4]: quatro threads no proprio processo. Nao ha cluster aqui --
        # o "executor" e a sua maquina.
        .master("local[4]")
        # O padrao e 200 particoes de shuffle. Com alguns milhares de linhas
        # isso e mais tempo agendando tarefa do que processando dado.
        .config("spark.sql.shuffle.partitions", "8")
        # A UI web do Spark (porta 4040) nao serve para nada num script que
        # dura 30 s, e cada sessao tenta abrir uma porta nova.
        .config("spark.ui.enabled", "false")
        # Sem isto o terminal enche de barras "[Stage 3:====>   (2 + 1) / 3]".
        # Sao inofensivas -- mas atrapalham quem esta lendo a saida pela
        # primeira vez.
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    # WARN do Hadoop no Windows sao ruido constante; o que interessa e ERROR.
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def titulo(texto: str) -> None:
    print(f"\n{'=' * 70}\n{texto}\n{'=' * 70}")
