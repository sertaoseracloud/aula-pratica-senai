"""Configuracao compartilhada pelos scripts deste laboratorio.

Tudo o que depende da maquina ou da AWS mora aqui: o interpretador dos
workers, o HADOOP_HOME do Windows, a sessao Spark e o cliente do S3. Os
scripts de exemplo ficam com o PySpark e o boto3, e nada mais.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"          # arquivos baixados do S3 para processar localmente
SAIDA = RAIZ / "saida"          # resultado do ETL, antes de subir de volta

# ---------------------------------------------------------------------------
# AWS S3
# ---------------------------------------------------------------------------
# Credenciais padrao do floci (https://floci.io/aws/) -- o emulador de AWS
# que roda em Docker (veja o SETUP.md). "test"/"test" NAO sao credenciais
# reais: sao as credenciais fixas que o emulador aceita sem checar nada. So
# funcionam contra um floci local, nunca contra uma conta AWS de verdade.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

FLOCI_ENDPOINT = "http://127.0.0.1:4566"

# AWS_ENDPOINT_URL nao definida         -> usa o floci local (padrao do laboratorio)
# AWS_ENDPOINT_URL="" (vazia)           -> usa a resolucao padrao do boto3 (AWS real)
# AWS_ENDPOINT_URL="<outra coisa>"      -> usa exatamente esse endereco
# E assim que "trocar de nuvem para producao" costuma acontecer de verdade:
# variavel de ambiente, nao edicao de codigo.
_endpoint_env = os.environ.get("AWS_ENDPOINT_URL")
AWS_ENDPOINT_URL = FLOCI_ENDPOINT if _endpoint_env is None else (_endpoint_env or None)

BUCKET = os.environ.get("AWS_S3_BUCKET", "energia-sc")


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
        # us-east-1 e a unica regiao que NAO aceita LocationConstraint --
        # mandar o parametro la da erro `InvalidLocationConstraint`.
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
    """Envia um arquivo local para o S3, sobrescrevendo se ja existir."""
    s3.upload_file(str(caminho_local), BUCKET, chave)


def baixar_arquivo(s3, chave: str, caminho_local: Path) -> None:
    """Baixa um objeto do S3 para um arquivo local, criando as pastas no caminho."""
    caminho_local.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(BUCKET, chave, str(caminho_local))


def subir_pasta(s3, pasta_local: Path, prefixo: str) -> int:
    """Envia todos os arquivos de dados de uma pasta (recursivo) para o S3.

    Usada depois do `write` do Spark: uma pasta Parquet particionada e um
    monte de arquivos em subpastas `municipio=.../part-....parquet`. O S3
    nao tem pasta de verdade -- cada arquivo vira um objeto cuja chave
    inclui as barras, o que basta para simular a mesma hierarquia.
    """
    enviados = 0
    for arquivo in pasta_local.rglob("*"):
        if not arquivo.is_file() or arquivo.name.startswith(("_", ".")):
            continue  # pula _SUCCESS e .crc: metadado do Hadoop, nao dado
        chave = f"{prefixo}/{arquivo.relative_to(pasta_local).as_posix()}"
        subir_arquivo(s3, arquivo, chave)
        enviados += 1
    return enviados


def preparar_ambiente() -> None:
    """Ajusta as variaveis que o Spark le antes de a JVM subir.

    Identico ao comum.py das outras aulas -- o S3 muda de onde o dado vem
    e vai, mas a JVM continua rodando local[4] na sua maquina.
    """
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
