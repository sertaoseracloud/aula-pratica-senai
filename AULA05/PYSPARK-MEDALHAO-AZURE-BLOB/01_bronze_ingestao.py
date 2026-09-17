"""Job 1/3 -- camada Bronze: ingestao crua da pluviometria, com proveniencia.

    python 01_bronze_ingestao.py

Igual ao job de Bronze do laboratorio 100% local (AULA05/PYSPARK-MEDALHAO),
com uma diferenca: a fonte e o destino desta camada vivem no Azure Blob
Storage, nao no seu disco. O disco aqui e so uma escala -- o Spark local nao
fala com o Blob diretamente, entao todo dado passa por download/upload
explicito com o SDK antes e depois do processamento.

Este e um job INDEPENDENTE: gera e semeia a fonte, baixa, acrescenta
proveniencia, sobe a Bronze para o Blob. O job da Silver
(02_silver_limpeza.py) le a Bronze QUE ESTE JOB GRAVOU NO BLOB -- nunca o
CSV original.

    dados/chuva_sc.csv                 entrada local (gerada e enviada uma vez)
    floci-az fonte/chuva_sc.csv        a mesma entrada, no Blob
    camadas/bronze/pluviometria/       copia local da Bronze, antes de subir
    floci-az bronze/pluviometria/      a Bronze, no Blob -- o que o job 2 le
"""

import csv
import datetime
import random
import shutil

from comum import (
    CAMADAS,
    DADOS,
    baixar_arquivo,
    criar_sessao,
    obter_container_client,
    subir_arquivo,
    subir_pasta,
    titulo,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ARQUIVO_CHUVA = DADOS / "chuva_sc.csv"

# municipio -> media diaria de chuva do trimestre, em mm (usada so na geracao)
MUNICIPIOS_CHUVA_BASE = {
    "Florianopolis": 6.0,
    "Sao Jose": 5.5,
    "Joinville": 7.0,
    "Jaragua do Sul": 6.5,
    "Blumenau": 7.5,
    "Itajai": 7.0,
    "Balneario Camboriu": 6.0,
    "Chapeco": 5.0,
    "Criciuma": 5.5,
    "Lages": 4.5,
}
NOMES_MUNICIPIOS = list(MUNICIPIOS_CHUVA_BASE)

DATA_INICIO = datetime.date(2024, 7, 1)
DATA_FIM = datetime.date(2024, 9, 30)          # trimestre: 92 dias, jul-set

SCHEMA_CHUVA = StructType(
    [
        StructField("leitura_id", IntegerType(), True),
        StructField("data", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("chuva_mm", DoubleType(), True),
        StructField("umidade_pct", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- dado, sem exercicio: gera o CSV e semeia o Blob
# ---------------------------------------------------------------------------
def gerar_csv_chuva() -> None:
    if ARQUIVO_CHUVA.exists():
        print(f"entrada ja existe: {ARQUIVO_CHUVA} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(43)               # semente fixa: numeros reproduziveis
    dias = (DATA_FIM - DATA_INICIO).days + 1     # 92

    cabecalho = ["leitura_id", "data", "municipio", "chuva_mm", "umidade_pct"]
    linhas = []
    leitura_id = 0
    for dia_idx in range(dias):
        data = DATA_INICIO + datetime.timedelta(days=dia_idx)
        for municipio, media in MUNICIPIOS_CHUVA_BASE.items():
            leitura_id += 1
            chove = aleatorio.random() < 0.35
            chuva = abs(aleatorio.gauss(media * 3, media * 2)) if chove else 0.0
            chuva = round(chuva, 1)
            umidade = round(max(0.0, min(100.0, aleatorio.gauss(80, 10))), 1)

            linha = [leitura_id, data.isoformat(), municipio, chuva, umidade]

            # ~5% de sujeira, dos tipos que um pluviometro de campo produz de verdade
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[2] = ""                            # municipio ausente
            elif sorteio < 0.02:
                linha[2] = linha[2].lower()               # municipio em formato errado
            elif sorteio < 0.03:
                linha[3] = -abs(linha[3]) - 1             # chuva negativa (erro de sensor)
            elif sorteio < 0.04:
                linha[3] = 350.0                          # chuva fora da faixa fisica
            elif sorteio < 0.05:
                linha[4] = 140.0 if aleatorio.random() < 0.5 else -5.0  # umidade fora de 0-100

            linhas.append(linha)
            if sorteio < 0.004:
                linhas.append(list(linha))                # duplicata exata

    with ARQUIVO_CHUVA.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    tamanho = ARQUIVO_CHUVA.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_CHUVA} ({len(linhas)} linhas, {tamanho:.0f} KB)")


def semear_blob(container) -> None:
    """Sobe a entrada bruta para o Blob, se ainda nao estiver la."""
    remoto = "fonte/chuva_sc.csv"
    if container.get_blob_client(remoto).exists():
        print(f"ja esta no Blob: {remoto} (nao reenviado)")
        return
    subir_arquivo(container, ARQUIVO_CHUVA, remoto)
    print(f"enviado ao Blob: {ARQUIVO_CHUVA.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar do Blob, so entao ler com o Spark
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, container) -> DataFrame:
    baixar_arquivo(container, "fonte/chuva_sc.csv", DADOS / "_baixado_chuva.csv")
    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_CHUVA)
        .csv(str(DADOS / "_baixado_chuva.csv"))
    )
    print("linhas baixadas do Blob:", bruto.count())
    bruto.show(5, truncate=False)
    return bruto


# ---------------------------------------------------------------------------
# O exercicio desta camada: proveniencia, nao limpeza
# ---------------------------------------------------------------------------
def enriquecer_proveniencia(bruto: DataFrame) -> DataFrame:
    """Exercicio 1 -- acrescentar de onde veio e quando chegou.

    A Bronze NAO valida, NAO normaliza, NAO remove duplicata -- isso e
    trabalho da Silver. O unico acrescimo aqui e proveniencia:

    - `arquivo_origem`: F.lit("fonte/chuva_sc.csv") -- o blob de onde esta
      leitura veio.
    - `ingerido_em`: F.current_timestamp() -- o instante em que este job rodou.

    Resposta esperada: 924 linhas (identico ao CSV bruto -- a Bronze nao
    filtra nada, so acrescenta duas colunas).
    """
    # TODO Exercicio 1: withColumn("arquivo_origem", ...), withColumn("ingerido_em", ...)
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar local, depois subir a Bronze
# ---------------------------------------------------------------------------
def carregar_bronze(bronze: DataFrame, container) -> None:
    destino = CAMADAS / "bronze" / "pluviometria"
    bronze.write.mode("overwrite").parquet(str(destino))

    arquivos = [
        p for p in destino.rglob("*")
        if p.is_file() and not p.name.startswith(("_", "."))
    ]
    print(f"Bronze gravada localmente em {destino} ({len(arquivos)} arquivo(s))")

    enviados = subir_pasta(container, destino, "bronze/pluviometria")
    print(f"Bronze enviada ao Blob: bronze/pluviometria/ ({enviados} arquivo(s))")


def main() -> None:
    spark = criar_sessao("aula05-blob-01-bronze")
    try:
        titulo("0. Fonte -- gerar e semear o Blob")
        gerar_csv_chuva()
        container = obter_container_client()
        semear_blob(container)

        titulo("1. Extract -- baixar do Blob e ler com o Spark")
        bruto = extrair(spark, container)

        titulo("2. Proveniencia (exercicio 1)")
        bronze = enriquecer_proveniencia(bruto)
        total_bronze = bronze.count()
        print(f"linhas na Bronze ..: {total_bronze}")
        bronze.show(5, truncate=False)

        titulo("3. Load -- gravar e subir a Bronze")
        carregar_bronze(bronze, container)

        titulo("Resumo do job")
        print(f"linhas na Bronze: {total_bronze}")
        print("proximo job .....: 02_silver_limpeza.py")
    finally:
        spark.stop()


def limpar_camadas() -> None:
    """Apaga a copia local das camadas, preservando a entrada."""
    if CAMADAS.exists():
        shutil.rmtree(CAMADAS)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_camadas()
        print(f"camadas locais removidas: {CAMADAS}")
    main()
