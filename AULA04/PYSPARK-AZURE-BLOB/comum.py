"""Configuracao compartilhada pelos scripts deste laboratorio.

Tudo o que depende da maquina ou do Azure mora aqui: o interpretador dos
workers, o HADOOP_HOME do Windows, a sessao Spark e o cliente do Blob
Storage. Os scripts de exemplo ficam com o PySpark e o SDK do Azure, e
nada mais.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"          # arquivos baixados do Blob para processar localmente
SAIDA = RAIZ / "saida"          # resultado do ETL, antes de subir de volta

# ---------------------------------------------------------------------------
# Azure Blob Storage
# ---------------------------------------------------------------------------
# String de conexao do floci-az (https://floci.io/az/) -- o emulador de Azure
# que roda em Docker (veja o SETUP.md). Esta NAO e uma credencial real: e a
# mesma chave de desenvolvimento publica e fixa que o Azurite tambem usa --
# floci-az e compativel de proposito, so muda a porta (4577 em vez de
# 10000). So funciona contra um emulador local, nunca contra uma conta do
# Azure de verdade.
CONNECTION_STRING_FLOCI_AZ = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:4577/devstoreaccount1;"
)

# Para apontar para uma conta real do Azure em vez do emulador, exporte a
# variavel de ambiente abaixo com a connection string da sua conta -- nenhum
# script muda. E assim que "trocar de nuvem para producao" costuma
# acontecer de verdade: variavel de ambiente, nao edicao de codigo.
AZURE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", CONNECTION_STRING_FLOCI_AZ
)
CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "temperaturas-sc")


def obter_container_client():
    """Devolve o ContainerClient do container do laboratorio, criando-o se preciso."""
    from azure.storage.blob import BlobServiceClient

    servico = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = servico.get_container_client(CONTAINER)
    if not container.exists():
        container.create_container()
    return container


def subir_arquivo(container, caminho_local: Path, nome_blob: str) -> None:
    """Envia um arquivo local para o Blob, sobrescrevendo se ja existir."""
    with caminho_local.open("rb") as fh:
        container.upload_blob(name=nome_blob, data=fh, overwrite=True)


def baixar_arquivo(container, nome_blob: str, caminho_local: Path) -> None:
    """Baixa um blob para um arquivo local, criando as pastas no caminho."""
    caminho_local.parent.mkdir(parents=True, exist_ok=True)
    fluxo = container.get_blob_client(nome_blob).download_blob()
    with caminho_local.open("wb") as fh:
        fh.write(fluxo.readall())


def subir_pasta(container, pasta_local: Path, prefixo_blob: str) -> int:
    """Envia todos os arquivos de dados de uma pasta (recursivo) para o Blob.

    Usada depois do `write` do Spark: uma pasta Parquet particionada e um
    monte de arquivos em subpastas `municipio=.../part-....parquet`. O Blob
    Storage nao tem pasta de verdade -- cada arquivo vira um blob cujo nome
    inclui as barras, o que basta para simular a mesma hierarquia.
    """
    enviados = 0
    for arquivo in pasta_local.rglob("*"):
        if not arquivo.is_file() or arquivo.name.startswith(("_", ".")):
            continue  # pula _SUCCESS e .crc: metadado do Hadoop, nao dado
        nome_blob = f"{prefixo_blob}/{arquivo.relative_to(pasta_local).as_posix()}"
        subir_arquivo(container, arquivo, nome_blob)
        enviados += 1
    return enviados


def preparar_ambiente() -> None:
    """Ajusta as variaveis que o Spark le antes de a JVM subir.

    Identico ao comum.py da AULA03 -- o Blob Storage muda de onde o dado
    vem e vai, mas a JVM continua rodando local[4] na sua maquina.
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
