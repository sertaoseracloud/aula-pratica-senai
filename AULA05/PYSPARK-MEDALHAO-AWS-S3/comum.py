"""Configuracao compartilhada pelos tres jobs do laboratorio.

Tudo o que depende da maquina ou da AWS mora aqui: o interpretador dos
workers, o HADOOP_HOME do Windows, a sessao Spark e o cliente do S3. Os
jobs de cada camada ficam com o PySpark e o boto3, e nada mais.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"              # arquivos baixados do S3 para processar localmente
CAMADAS = RAIZ / "camadas"          # copia local das tres camadas, espelhando o S3

# ---------------------------------------------------------------------------
# AWS S3
# ---------------------------------------------------------------------------
# Mesmas credenciais fixas do laboratorio PYSPARK-AWS-S3 da AULA04 -- so
# funcionam contra um emulador local (floci).
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

FLOCI_ENDPOINT = "http://127.0.0.1:4566"

# AWS_ENDPOINT_URL nao definida    -> usa o floci local (padrao do laboratorio)
# AWS_ENDPOINT_URL="" (vazia)      -> usa a resolucao padrao do boto3 (AWS real)
_endpoint_env = os.environ.get("AWS_ENDPOINT_URL")
AWS_ENDPOINT_URL = FLOCI_ENDPOINT if _endpoint_env is None else (_endpoint_env or None)

BUCKET = os.environ.get("AWS_S3_BUCKET", "medalhao-residuos-sc")


def obter_bucket():
    """Devolve um cliente S3 e garante que o bucket do laboratorio existe."""
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3", endpoint_url=AWS_ENDPOINT_URL)
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        argumentos = {"Bucket": BUCKET}
        regiao = os.environ["AWS_DEFAULT_REGION"]
        if regiao != "us-east-1":
            argumentos["CreateBucketConfiguration"] = {"LocationConstraint": regiao}
        s3.create_bucket(**argumentos)
    return s3


def objeto_existe(s3, chave: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        s3.head_object(Bucket=BUCKET, Key=chave)
        return True
    except ClientError:
        return False


def subir_arquivo(s3, caminho_local: Path, chave: str) -> None:
    s3.upload_file(str(caminho_local), BUCKET, chave)


def baixar_arquivo(s3, chave: str, caminho_local: Path) -> None:
    caminho_local.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(BUCKET, chave, str(caminho_local))


def subir_pasta(s3, pasta_local: Path, prefixo: str) -> int:
    """Envia todos os arquivos de dados de uma pasta Parquet (recursivo) para o S3."""
    enviados = 0
    for arquivo in pasta_local.rglob("*"):
        if not arquivo.is_file() or arquivo.name.startswith(("_", ".")):
            continue
        chave = f"{prefixo}/{arquivo.relative_to(pasta_local).as_posix()}"
        subir_arquivo(s3, arquivo, chave)
        enviados += 1
    return enviados


def baixar_pasta(s3, prefixo: str, pasta_local: Path) -> int:
    """Baixa todos os objetos de um prefixo para uma pasta local, espelhando a arvore.

    O S3 devolve no maximo 1000 chaves por chamada -- para as camadas deste
    laboratorio (poucas dezenas de arquivos Parquet) isso nunca chega perto
    do limite, mas um pipeline maior precisaria paginar com `ContinuationToken`.
    """
    baixados = 0
    paginador = s3.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=f"{prefixo}/"):
        for objeto in pagina.get("Contents", []):
            relativo = objeto["Key"][len(prefixo) + 1:]
            baixar_arquivo(s3, objeto["Key"], pasta_local / relativo)
            baixados += 1
    return baixados


def prefixo_existe(s3, prefixo: str) -> bool:
    """True se existir pelo menos um objeto sob esse prefixo."""
    resposta = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{prefixo}/", MaxKeys=1)
    return resposta.get("KeyCount", 0) > 0


def preparar_ambiente() -> None:
    """Ajusta as variaveis que o Spark le antes de a JVM subir."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name == "nt":
        hadoop = RAIZ / "hadoop"
        if not (hadoop / "bin" / "winutils.exe").exists():
            raise SystemExit(
                "Falta hadoop/bin/winutils.exe. Rode o Passo 4 do SETUP.md."
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
