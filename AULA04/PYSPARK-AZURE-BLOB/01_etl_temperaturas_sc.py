"""ETL completo -- temperaturas de um trimestre em dez cidades de SC, lidas e
gravadas no Azure Blob Storage.

    python 01_etl_temperaturas_sc.py

Este e o `03_etl_local.py` da AULA03 com uma diferenca: a entrada nao esta
no seu disco, esta num container de Blob Storage, e a saida volta pra la
depois de processada. As cinco etapas continuam as mesmas -- Extract,
Transform, Load -- só que Extract e Load agora cruzam a rede (aqui, o
floci-az local; em produção, o Azure de verdade, trocando só a connection
string).

O padrao usado aqui -- baixar do Blob, processar local, subir o resultado
de volta -- e o mesmo que um job Spark de no unico (sem cluster distribuido)
usa na pratica: o `local[4]` deste laboratorio nao enxerga o Blob como um
sistema de arquivos particionavel, entao ele baixa o objeto inteiro, como
qualquer outro programa faria.

    dados/temperaturas_sc.csv                entrada local (gerada e enviada uma vez)
    dados/municipios_sc.csv                  tabela de apoio (municipio -> regiao)
    floci-az bruto/temperaturas_sc.csv       a mesma entrada, no Blob
    floci-az referencia/municipios_sc.csv    a mesma tabela, no Blob
    saida/temperaturas/municipio=.../        fato limpo, local
    floci-az processado/temperaturas/...     o mesmo fato, subido de volta
    floci-az processado/resumo_regiao/...    media trimestral por regiao
    floci-az processado/temp_rejeitadas/...  linhas que nao passaram na validacao
"""

import csv
import datetime
import random
import shutil

from comum import (
    DADOS,
    SAIDA,
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

ARQUIVO_TEMPERATURAS = DADOS / "temperaturas_sc.csv"
ARQUIVO_MUNICIPIOS = DADOS / "municipios_sc.csv"

# municipio -> (regiao, temperatura media base do trimestre, em graus C)
MUNICIPIOS_REGIOES = {
    "Florianopolis": ("Grande Florianopolis", 18.0),
    "Sao Jose": ("Grande Florianopolis", 18.0),
    "Joinville": ("Norte Catarinense", 18.0),
    "Jaragua do Sul": ("Norte Catarinense", 17.0),
    "Blumenau": ("Vale do Itajai", 17.0),
    "Itajai": ("Vale do Itajai", 18.0),
    "Balneario Camboriu": ("Vale do Itajai", 19.0),
    "Chapeco": ("Oeste Catarinense", 16.0),
    "Criciuma": ("Sul Catarinense", 17.0),
    "Lages": ("Serrana", 13.0),
}
NOMES_MUNICIPIOS = list(MUNICIPIOS_REGIOES)

DATA_INICIO = datetime.date(2024, 7, 1)
DATA_FIM = datetime.date(2024, 9, 30)          # trimestre: 92 dias, jul-set (inverno -> primavera)

SCHEMA_TEMPERATURAS = StructType(
    [
        StructField("estacao_id", IntegerType(), True),
        StructField("data", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("temperatura_min", DoubleType(), True),
        StructField("temperatura_max", DoubleType(), True),
        StructField("temperatura_media", DoubleType(), True),
        StructField("umidade_pct", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- gera os dois CSVs localmente, uma unica vez
# ---------------------------------------------------------------------------
def gerar_csv_temperaturas() -> None:
    if ARQUIVO_TEMPERATURAS.exists():
        print(f"entrada ja existe: {ARQUIVO_TEMPERATURAS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(7)               # semente fixa: numeros reproduziveis
    dias = (DATA_FIM - DATA_INICIO).days + 1    # 92

    cabecalho = [
        "estacao_id", "data", "municipio", "temperatura_min",
        "temperatura_max", "temperatura_media", "umidade_pct",
    ]
    linhas = []
    estacao_id = 0
    for dia_idx in range(dias):
        data = DATA_INICIO + datetime.timedelta(days=dia_idx)
        # tendencia sazonal: sobe ~3 graus do inicio (inverno) ao fim (primavera)
        tendencia = 3.0 * (dia_idx / (dias - 1))
        for municipio in NOMES_MUNICIPIOS:
            estacao_id += 1
            _, base = MUNICIPIOS_REGIOES[municipio]
            media = base + tendencia + aleatorio.gauss(0, 2.5)
            variacao = aleatorio.uniform(3, 8)
            temp_min = round(media - variacao, 1)
            temp_max = round(media + variacao, 1)
            temp_media = round((temp_min + temp_max) / 2, 1)
            umidade = round(max(0.0, min(100.0, aleatorio.gauss(78, 10))), 1)

            linha = [estacao_id, data.isoformat(), municipio, temp_min, temp_max, temp_media, umidade]

            # ~5% de sujeira, dos tipos que um sensor de campo produz de verdade
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[2] = ""                            # municipio ausente
            elif sorteio < 0.02:
                linha[2] = linha[2].lower()               # municipio em formato errado
            elif sorteio < 0.03:
                linha[3], linha[4] = linha[4], linha[3]   # min/max trocados (sensor invertido)
            elif sorteio < 0.04:
                linha[4] = 65.0 if aleatorio.random() < 0.5 else -25.0   # fora da faixa fisica
            elif sorteio < 0.05:
                linha[6] = 130.0 if aleatorio.random() < 0.5 else -10.0  # umidade fora de 0-100

            linhas.append(linha)
            if sorteio < 0.004:
                linhas.append(list(linha))                # duplicata exata

    with ARQUIVO_TEMPERATURAS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    tamanho = ARQUIVO_TEMPERATURAS.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_TEMPERATURAS} ({len(linhas)} linhas, {tamanho:.0f} KB)")


def gerar_csv_municipios() -> None:
    if ARQUIVO_MUNICIPIOS.exists():
        print(f"entrada ja existe: {ARQUIVO_MUNICIPIOS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    with ARQUIVO_MUNICIPIOS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(["municipio", "regiao"])
        for municipio, (regiao, _) in MUNICIPIOS_REGIOES.items():
            escritor.writerow([municipio.upper(), regiao])

    print(f"entrada gerada: {ARQUIVO_MUNICIPIOS}")


def semear_blob(container) -> None:
    """Sobe as duas entradas para o Blob, se ainda nao estiverem la.

    Em produção alguem (ou algum outro pipeline) ja teria colocado o dado no
    Blob antes do seu job rodar. Aqui essa etapa e simulada uma unica vez --
    depois disso, o restante do script trata o Blob como a fonte de
    verdade, sem olhar para o arquivo local de novo.
    """
    for local, remoto in (
        (ARQUIVO_TEMPERATURAS, "bruto/temperaturas_sc.csv"),
        (ARQUIVO_MUNICIPIOS, "referencia/municipios_sc.csv"),
    ):
        if container.get_blob_client(remoto).exists():
            print(f"ja esta no Blob: {remoto} (nao reenviado)")
            continue
        subir_arquivo(container, local, remoto)
        print(f"enviado ao Blob: {local.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- baixar do Blob, so entao ler com o Spark
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, container) -> tuple[DataFrame, DataFrame]:
    # O Spark local nao fala com o Blob Storage diretamente -- ele le do
    # disco. Por isso o download vem antes do `spark.read`, nao dentro dele.
    baixar_arquivo(container, "bruto/temperaturas_sc.csv", DADOS / "_baixado_temperaturas.csv")
    baixar_arquivo(container, "referencia/municipios_sc.csv", DADOS / "_baixado_municipios.csv")

    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_TEMPERATURAS)
        .csv(str(DADOS / "_baixado_temperaturas.csv"))
    )
    municipios = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(DADOS / "_baixado_municipios.csv"))
    )
    print("linhas baixadas do Blob (temperaturas):", bruto.count())
    print("linhas baixadas do Blob (municipios) ..:", municipios.count())
    bruto.show(5, truncate=False)
    return bruto, municipios


# ---------------------------------------------------------------------------
# T de Transform -- identico em espirito ao 03_etl_local.py
# ---------------------------------------------------------------------------
def transformar(
    bruto: DataFrame, municipios: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    normalizado = (
        bruto
        .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
        .withColumn("data", F.to_date("data", "yyyy-MM-dd"))
        .dropDuplicates(["estacao_id", "data", "municipio"])
    )

    nomes_validos = [m.upper() for m in NOMES_MUNICIPIOS]

    # F.coalesce(..., F.lit(False)): sem ele, uma linha com `municipio` nulo
    # faria `isin(...)` avaliar para NULL, e `~NULL` tambem e NULL -- a linha
    # sumiria dos dois lados (aprovados e rejeitados) sem que a conciliacao
    # notasse, ate voce somar os dois totais e sobrar uma diferenca muda.
    regra_valida = F.coalesce(
        F.col("municipio").isin(nomes_validos)
        & (F.col("temperatura_min") <= F.col("temperatura_max"))
        & F.col("temperatura_min").between(-15, 50)
        & F.col("temperatura_max").between(-15, 50)
        & F.col("umidade_pct").between(0, 100),
        F.lit(False),
    )

    rejeitadas = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.when(~F.col("municipio").isin(nomes_validos), "municipio invalida ou ausente")
         .when(F.col("temperatura_min") > F.col("temperatura_max"), "temperatura_min maior que temperatura_max")
         .when(
             ~F.col("temperatura_min").between(-15, 50) | ~F.col("temperatura_max").between(-15, 50),
             "temperatura fora da faixa fisica (-15 a 50)",
         )
         .otherwise("umidade fora da faixa (0 a 100)"),
    )

    limpo = (
        normalizado.filter(regra_valida)
        .withColumn("amplitude_termica", F.round(F.col("temperatura_max") - F.col("temperatura_min"), 1))
        .withColumn(
            "faixa_dia",
            F.when(F.col("temperatura_media") < 15, "fria")
             .when(F.col("temperatura_media") <= 22, "amena")
             .otherwise("quente"),
        )
        .join(municipios, on="municipio", how="left")
        .select(
            "estacao_id", "data", "municipio", "regiao",
            "temperatura_min", "temperatura_max", "temperatura_media",
            "amplitude_termica", "umidade_pct", "faixa_dia",
        )
    )

    resumo_regiao = (
        limpo.groupBy("regiao")
        .agg(
            F.count("*").alias("leituras"),
            F.round(F.avg("temperatura_media"), 2).alias("media_trimestral"),
            F.round(F.avg("amplitude_termica"), 2).alias("amplitude_media"),
        )
        .orderBy(F.desc("media_trimestral"))
    )

    return limpo, resumo_regiao, rejeitadas, normalizado


# ---------------------------------------------------------------------------
# L de Load -- gravar local, depois subir o resultado para o Blob
# ---------------------------------------------------------------------------
def carregar(limpo: DataFrame, resumo_regiao: DataFrame, rejeitadas: DataFrame, container) -> None:
    destino_fato = SAIDA / "temperaturas"
    destino_resumo = SAIDA / "resumo_regiao"
    destino_rejeitadas = SAIDA / "temp_rejeitadas"

    (
        limpo
        .repartition("municipio")
        .write.mode("overwrite")
        .partitionBy("municipio")
        .parquet(str(destino_fato))
    )
    resumo_regiao.coalesce(1).write.mode("overwrite").parquet(str(destino_resumo))
    rejeitadas.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitadas)
    )

    for pasta in (destino_fato, destino_resumo, destino_rejeitadas):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<16} {len(arquivos):>3} arquivo(s) de dados (local)")

    # So agora o resultado sobe para o Blob -- o Spark nunca escreveu la
    # diretamente, porque `local[4]` nao tem um conector de sistema de
    # arquivos para o Blob configurado (veja o README, secao "por que baixar
    # em vez de ler direto").
    total_enviado = 0
    for pasta, prefixo in (
        (destino_fato, "processado/temperaturas"),
        (destino_resumo, "processado/resumo_regiao"),
        (destino_rejeitadas, "processado/temp_rejeitadas"),
    ):
        total_enviado += subir_pasta(container, pasta, prefixo)
    print(f"arquivos enviados ao Blob (processado/*): {total_enviado}")


def verificar(container) -> None:
    """Prova de ponta a ponta: o que esta no Blob depois do Load e legivel."""
    blobs_resumo = [
        b.name for b in container.list_blobs(name_starts_with="processado/resumo_regiao/")
        if b.name.endswith(".parquet")
    ]
    print(f"arquivo(s) de resumo no Blob: {blobs_resumo}")
    if not blobs_resumo:
        raise SystemExit("nenhum parquet de resumo encontrado no Blob -- o Load falhou")

    destino = DADOS / "_verificacao_resumo.parquet"
    baixar_arquivo(container, blobs_resumo[0], destino)
    print(f"baixado de volta do Blob para conferencia: {destino} ({destino.stat().st_size} bytes)")


def main() -> None:
    spark = criar_sessao("aula04-01-etl-temperaturas-blob")
    try:
        titulo("0. Fonte -- gerar e semear o Blob")
        gerar_csv_temperaturas()
        gerar_csv_municipios()
        container = obter_container_client()
        semear_blob(container)

        titulo("1. Extract -- baixar do Blob e ler com o Spark")
        bruto, municipios = extrair(spark, container)

        titulo("2. Transform -- normalizar, validar, enriquecer")
        limpo, resumo_regiao, rejeitadas, normalizado = transformar(bruto, municipios)
        limpo.cache()
        total_bruto = bruto.count()
        total_normalizado = normalizado.count()
        total_limpo = limpo.count()
        total_rejeitado = rejeitadas.count()
        print(f"lidas .............: {total_bruto}")
        print(f"apos dropDuplicates: {total_normalizado}"
              f"  (-{total_bruto - total_normalizado} duplicatas)")
        print(f"aprovadas .........: {total_limpo}")
        print(f"rejeitadas ........: {total_rejeitado}")
        # Conciliacao: toda linha normalizada tem de sair de um dos dois lados.
        assert total_limpo + total_rejeitado == total_normalizado, (
            f"linhas perdidas: {total_normalizado - total_limpo - total_rejeitado}"
        )
        print("conciliacao .......: OK")
        rejeitadas.groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)
        limpo.show(5)

        titulo("3. Load -- gravar local e subir para o Blob")
        carregar(limpo, resumo_regiao, rejeitadas, container)

        titulo("4. Verificacao -- ler de volta do Blob")
        verificar(container)

        titulo("5. Resumo por regiao")
        resumo_regiao.show()

        titulo("Resumo da execucao")
        print(f"container ....: {container.container_name}")
        print(f"lidas ........: {total_bruto}")
        print(f"aprovadas ....: {total_limpo}")
        print(f"rejeitadas ...: {total_rejeitado}")
        print(f"saida local ..: {SAIDA}")
        print("saida no Blob : processado/temperaturas, processado/resumo_regiao, "
              "processado/temp_rejeitadas")
    finally:
        spark.stop()


def limpar_saida() -> None:
    """Apaga a saida local, preservando a entrada -- util para reexecutar do zero."""
    if SAIDA.exists():
        shutil.rmtree(SAIDA)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_saida()
        print(f"saida local removida: {SAIDA}")
    main()
