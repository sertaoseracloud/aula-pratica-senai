"""Configuracao compartilhada pelos tres jobs do laboratorio.

Tudo o que depende da maquina ou do Azure mora aqui: o interpretador dos
workers, o HADOOP_HOME do Windows, a sessao Spark e o cliente do Blob
Storage. Os jobs de cada camada ficam com o PySpark e o SDK do Azure, e
nada mais.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"              # arquivos baixados do Blob para processar localmente
CAMADAS = RAIZ / "camadas"          # copia local das tres camadas, espelhando o Blob

# ---------------------------------------------------------------------------
# Azure Blob Storage
# ---------------------------------------------------------------------------
# Mesma chave de desenvolvimento do laboratorio PYSPARK-AZURE-BLOB da AULA04
# -- publica, fixa, e so funciona contra um emulador local (floci-az).
CONNECTION_STRING_FLOCI_AZ = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:4577/devstoreaccount1;"
)

AZURE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", CONNECTION_STRING_FLOCI_AZ
)
CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "medalhao-clima-sc")


def obter_container_client():
    """Devolve o ContainerClient do container do laboratorio, criando-o se preciso."""
    from azure.storage.blob import BlobServiceClient

    servico = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = servico.get_container_client(CONTAINER)
    if not container.exists():
        container.create_container()
    return container


def subir_arquivo(container, caminho_local: Path, nome_blob: str) -> None:
    with caminho_local.open("rb") as fh:
        container.upload_blob(name=nome_blob, data=fh, overwrite=True)


def baixar_arquivo(container, nome_blob: str, caminho_local: Path) -> None:
    caminho_local.parent.mkdir(parents=True, exist_ok=True)
    fluxo = container.get_blob_client(nome_blob).download_blob()
    with caminho_local.open("wb") as fh:
        fh.write(fluxo.readall())


def subir_pasta(container, pasta_local: Path, prefixo_blob: str) -> int:
    """Envia todos os arquivos de dados de uma pasta Parquet (recursivo) para o Blob."""
    enviados = 0
    for arquivo in pasta_local.rglob("*"):
        if not arquivo.is_file() or arquivo.name.startswith(("_", ".")):
            continue  # pula _SUCCESS e .crc: metadado do Hadoop, nao dado
        nome_blob = f"{prefixo_blob}/{arquivo.relative_to(pasta_local).as_posix()}"
        subir_arquivo(container, arquivo, nome_blob)
        enviados += 1
    return enviados


def baixar_pasta(container, prefixo_blob: str, pasta_local: Path) -> int:
    """Baixa todos os blobs de um prefixo para uma pasta local, espelhando a arvore.

    E o inverso de `subir_pasta`: usada para trazer uma camada Parquet
    inteira (varios arquivos `part-....parquet`) de volta para o disco antes
    de o Spark le-la -- o mesmo motivo do `baixar_arquivo` no laboratorio de
    ETL da AULA04, so que para uma pasta em vez de um arquivo so.
    """
    baixados = 0
    for blob in container.list_blobs(name_starts_with=f"{prefixo_blob}/"):
        relativo = blob.name[len(prefixo_blob) + 1:]
        baixar_arquivo(container, blob.name, pasta_local / relativo)
        baixados += 1
    return baixados


def prefixo_existe(container, prefixo_blob: str) -> bool:
    """True se existir pelo menos um blob sob esse prefixo -- usado para checar
    se a camada anterior ja foi gravada antes de tentar baixa-la."""
    return any(container.list_blobs(name_starts_with=f"{prefixo_blob}/"))


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
