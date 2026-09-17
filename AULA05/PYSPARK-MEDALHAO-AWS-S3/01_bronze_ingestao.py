"""Job 1/3 -- camada Bronze: ingestao crua da coleta de residuos, com proveniencia.

    python 01_bronze_ingestao.py

Igual ao job de Bronze do laboratorio 100% local (AULA05/PYSPARK-MEDALHAO),
com a fonte e o destino desta camada vivendo no S3 (emulado pelo floci), nao
no seu disco. O disco aqui e so escala -- todo dado passa por
download/upload explicito com o boto3 antes e depois do processamento.

Este e um job INDEPENDENTE: gera e semeia a fonte, baixa, acrescenta
proveniencia, sobe a Bronze para o S3. O job da Silver
(02_silver_limpeza.py) le a Bronze QUE ESTE JOB GRAVOU NO S3 -- nunca o
CSV original.

    dados/residuos_sc.csv          entrada local (gerada e enviada uma vez)
    S3 fonte/residuos_sc.csv       a mesma entrada, no bucket
    camadas/bronze/residuos/       copia local da Bronze, antes de subir
    S3 bronze/residuos/            a Bronze, no bucket -- o que o job 2 le
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
    obter_bucket,
    objeto_existe,
    subir_arquivo,
    subir_pasta,
    titulo,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ARQUIVO_RESIDUOS = DADOS / "residuos_sc.csv"

# municipio -> toneladas totais base do dia (usada so na geracao)
MUNICIPIOS_TONELADAS_BASE = {
    "Florianopolis": 420.0,
    "Sao Jose": 210.0,
    "Joinville": 480.0,
    "Jaragua do Sul": 150.0,
    "Blumenau": 290.0,
    "Itajai": 180.0,
    "Balneario Camboriu": 115.0,
    "Chapeco": 180.0,
    "Criciuma": 175.0,
    "Lages": 125.0,
}
NOMES_MUNICIPIOS = list(MUNICIPIOS_TONELADAS_BASE)

DATA_INICIO = datetime.date(2024, 7, 1)
DATA_FIM = datetime.date(2024, 9, 30)          # trimestre: 92 dias, jul-set

SCHEMA_RESIDUOS = StructType(
    [
        StructField("leitura_id", IntegerType(), True),
        StructField("data", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("toneladas_total", DoubleType(), True),
        StructField("toneladas_reciclavel", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- dado, sem exercicio: gera o CSV e semeia o S3
# ---------------------------------------------------------------------------
def gerar_csv_residuos() -> None:
    if ARQUIVO_RESIDUOS.exists():
        print(f"entrada ja existe: {ARQUIVO_RESIDUOS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(53)               # semente fixa: numeros reproduziveis
    dias = (DATA_FIM - DATA_INICIO).days + 1     # 92

    cabecalho = ["leitura_id", "data", "municipio", "toneladas_total", "toneladas_reciclavel"]
    linhas = []
    leitura_id = 0
    for dia_idx in range(dias):
        data = DATA_INICIO + datetime.timedelta(days=dia_idx)
        for municipio, base in MUNICIPIOS_TONELADAS_BASE.items():
            leitura_id += 1
            total = max(1.0, base * aleatorio.gauss(1.0, 0.10))
            taxa_reciclagem = max(0.03, min(0.45, aleatorio.gauss(0.18, 0.06)))
            reciclavel = total * taxa_reciclagem
            total = round(total, 1)
            reciclavel = round(reciclavel, 1)

            linha = [leitura_id, data.isoformat(), municipio, total, reciclavel]

            # ~5% de sujeira, dos tipos que uma balanca de caminhao de coleta produz de verdade
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[2] = ""                            # municipio ausente
            elif sorteio < 0.02:
                linha[2] = linha[2].lower()               # municipio em formato errado
            elif sorteio < 0.03:
                linha[3], linha[4] = linha[4], linha[3]   # trocados (reciclavel > total)
            elif sorteio < 0.04:
                linha[3] = -abs(linha[3])                 # total negativo (erro de balanca)
            elif sorteio < 0.05:
                linha[3] = 650.0                          # fora da faixa fisica (rota contada 2x)

            linhas.append(linha)
            if sorteio < 0.004:
                linhas.append(list(linha))                # duplicata exata

    with ARQUIVO_RESIDUOS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    tamanho = ARQUIVO_RESIDUOS.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_RESIDUOS} ({len(linhas)} linhas, {tamanho:.0f} KB)")


def semear_s3(s3) -> None:
    remoto = "fonte/residuos_sc.csv"
    if objeto_existe(s3, remoto):
        print(f"ja esta no S3: {remoto} (nao reenviado)")
        return
    subir_arquivo(s3, ARQUIVO_RESIDUOS, remoto)
    print(f"enviado ao S3: {ARQUIVO_RESIDUOS.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar do S3, so entao ler com o Spark
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, s3) -> DataFrame:
    baixar_arquivo(s3, "fonte/residuos_sc.csv", DADOS / "_baixado_residuos.csv")
    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_RESIDUOS)
        .csv(str(DADOS / "_baixado_residuos.csv"))
    )
    print("linhas baixadas do S3:", bruto.count())
    bruto.show(5, truncate=False)
    return bruto


# ---------------------------------------------------------------------------
# O exercicio desta camada: proveniencia, nao limpeza
# ---------------------------------------------------------------------------
def enriquecer_proveniencia(bruto: DataFrame) -> DataFrame:
    """Exercicio 1 -- acrescentar de onde veio e quando chegou.

    A Bronze NAO valida, NAO normaliza, NAO remove duplicata -- isso e
    trabalho da Silver. O unico acrescimo aqui e proveniencia:

    - `arquivo_origem`: F.lit("fonte/residuos_sc.csv") -- o objeto de onde
      esta leitura veio.
    - `ingerido_em`: F.current_timestamp() -- o instante em que este job rodou.

    Resposta esperada: 921 linhas (identico ao CSV bruto -- a Bronze nao
    filtra nada, so acrescenta duas colunas).
    """
    # TODO Exercicio 1: withColumn("arquivo_origem", ...), withColumn("ingerido_em", ...)
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar local, depois subir a Bronze
# ---------------------------------------------------------------------------
def carregar_bronze(bronze: DataFrame, s3) -> None:
    destino = CAMADAS / "bronze" / "residuos"
    bronze.write.mode("overwrite").parquet(str(destino))

    arquivos = [
        p for p in destino.rglob("*")
        if p.is_file() and not p.name.startswith(("_", "."))
    ]
    print(f"Bronze gravada localmente em {destino} ({len(arquivos)} arquivo(s))")

    enviados = subir_pasta(s3, destino, "bronze/residuos")
    print(f"Bronze enviada ao S3: bronze/residuos/ ({enviados} arquivo(s))")


def main() -> None:
    spark = criar_sessao("aula05-s3-01-bronze")
    try:
        titulo("0. Fonte -- gerar e semear o S3")
        gerar_csv_residuos()
        s3 = obter_bucket()
        semear_s3(s3)

        titulo("1. Extract -- baixar do S3 e ler com o Spark")
        bruto = extrair(spark, s3)

        titulo("2. Proveniencia (exercicio 1)")
        bronze = enriquecer_proveniencia(bruto)
        total_bronze = bronze.count()
        print(f"linhas na Bronze ..: {total_bronze}")
        bronze.show(5, truncate=False)

        titulo("3. Load -- gravar e subir a Bronze")
        carregar_bronze(bronze, s3)

        titulo("Resumo do job")
        print(f"linhas na Bronze: {total_bronze}")
        print("proximo job .....: 02_silver_limpeza.py")
    finally:
        spark.stop()


def limpar_camadas() -> None:
    if CAMADAS.exists():
        shutil.rmtree(CAMADAS)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_camadas()
        print(f"camadas locais removidas: {CAMADAS}")
    main()
