"""Exemplo 3 -- um ETL local completo, do CSV bruto ao Parquet particionado.

    python 03_etl_local.py

ETL = Extract, Transform, Load: ler de uma fonte, tratar, gravar num destino.
As tres etapas estao em funcoes separadas de proposito -- e assim que se testa
cada uma isoladamente, e assim que se descobre qual delas quebrou.

O script e IDEMPOTENTE: rodar duas vezes seguidas produz exatamente o mesmo
resultado. Isso nao acontece por acaso -- exige tres coisas, todas marcadas
com comentario no codigo: entrada gerada com semente fixa e so uma vez,
`mode("overwrite")` em toda escrita, e nenhuma dependencia da hora atual.

    dados/vendas_brutas.csv        entrada (gerada na primeira execucao)
    saida/vendas/uf=SP/*.parquet   fato limpo, particionado por UF
    saida/resumo_mensal/           agregado por UF, mes e categoria
    saida/rejeitados/              linhas que nao passaram na validacao
"""

import csv
import random
import shutil
import time

from comum import DADOS, SAIDA, criar_sessao, titulo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ARQUIVO_BRUTO = DADOS / "vendas_brutas.csv"

# O schema declarado vale mais do que parece num CSV: sem ele o Spark faz uma
# passada inteira so para inferir tipos, e ainda assim uma unica linha suja
# transforma a coluna toda em string.
SCHEMA_BRUTO = StructType(
    [
        StructField("pedido_id", IntegerType(), True),
        StructField("data_venda", StringType(), True),
        StructField("cliente", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("quantidade", IntegerType(), True),
        StructField("preco", DoubleType(), True),
    ]
)

UFS = ["SP", "RJ", "MG", "BA", "PE", "RS"]
CATEGORIAS = ["eletronicos", "livros", "moveis", "vestuario"]


# ---------------------------------------------------------------------------
# Fonte -- gera o CSV bruto uma unica vez, com a sujeira que um ETL encontra
# ---------------------------------------------------------------------------
def gerar_csv_bruto(linhas: int = 50_000) -> None:
    if ARQUIVO_BRUTO.exists():
        print(f"entrada ja existe: {ARQUIVO_BRUTO} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(42)          # semente fixa: numeros reproduziveis
    with ARQUIVO_BRUTO.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(
            ["pedido_id", "data_venda", "cliente", "uf", "categoria", "quantidade", "preco"]
        )
        for pedido_id in range(1, linhas + 1):
            dia = aleatorio.randint(1, 28)
            mes = aleatorio.randint(1, 12)
            linha = [
                pedido_id,
                f"2024-{mes:02d}-{dia:02d}",
                f"cliente_{aleatorio.randint(1, 900)}",
                aleatorio.choice(UFS),
                aleatorio.choice(CATEGORIAS),
                aleatorio.randint(1, 5),
                round(aleatorio.uniform(10, 3000), 2),
            ]
            # 4% de sujeira, dos tipos que aparecem de verdade em arquivo real
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[3] = ""                      # UF ausente
            elif sorteio < 0.02:
                linha[5] = -aleatorio.randint(1, 5)  # quantidade negativa
            elif sorteio < 0.03:
                linha[1] = "31/02/2024"            # data em outro formato
            elif sorteio < 0.04:
                linha[3] = linha[3].lower()        # UF em minuscula
            escritor.writerow(linha)

            if sorteio < 0.005:                    # duplicata exata
                escritor.writerow(linha)

    tamanho = ARQUIVO_BRUTO.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_BRUTO} ({tamanho:.0f} KB)")


# ---------------------------------------------------------------------------
# E de Extract
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession) -> DataFrame:
    df = (
        spark.read
        .option("header", "true")
        # PERMISSIVE (padrao) poe nulo no campo ruim e segue; DROPMALFORMED
        # descarta a linha calada; FAILFAST aborta. Aqui queremos ver o que
        # entrou errado, entao ficamos com o padrao e validamos depois.
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_BRUTO)
        .csv(str(ARQUIVO_BRUTO))
    )
    print("linhas lidas ....:", df.count())
    print("particoes .......:", df.rdd.getNumPartitions())
    df.show(5, truncate=False)
    return df


# ---------------------------------------------------------------------------
# T de Transform -- separa o que serve do que nao serve
# ---------------------------------------------------------------------------
def transformar(bruto: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    normalizado = (
        bruto
        .withColumn("uf", F.upper(F.trim(F.col("uf"))))
        .withColumn("cliente", F.trim(F.col("cliente")))
        # to_date devolve NULL quando a string nao casa com o formato -- e por
        # isso que "31/02/2024" vira nulo em vez de derrubar o job.
        .withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))
        .dropDuplicates(["pedido_id", "data_venda", "cliente", "uf", "categoria"])
    )

    # `col.isin(...)` devolve NULL quando a coluna e nula -- nao devolve falso.
    # E NULL negado continua NULL, que o `filter` descarta. Sem o coalesce
    # abaixo, as linhas de UF ausente sumiriam dos DOIS lados: nem aprovadas
    # nem rejeitadas. Foram 505 linhas evaporando silenciosamente na primeira
    # versao deste script -- so a conciliacao do total revelou.
    regra_valida = F.coalesce(
        F.col("pedido_id").isNotNull()
        & F.col("data_venda").isNotNull()
        & (F.col("uf").isin(UFS))
        & (F.col("quantidade") > 0)
        & (F.col("preco") > 0),
        F.lit(False),
    )

    rejeitados = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.when(F.col("data_venda").isNull(), "data invalida")
         .when(F.col("uf").isNull() | ~F.col("uf").isin(UFS), "uf invalida ou ausente")
         .when(F.col("quantidade") <= 0, "quantidade nao positiva")
         .when(F.col("preco") <= 0, "preco nao positivo")
         .when(F.col("pedido_id").isNull(), "pedido_id ausente")
         .otherwise("outro"),
    )

    limpo = (
        normalizado.filter(regra_valida)
        .withColumn("total", F.round(F.col("quantidade") * F.col("preco"), 2))
        .withColumn("ano", F.year("data_venda"))
        .withColumn("mes", F.month("data_venda"))
        .withColumn(
            "faixa_valor",
            F.when(F.col("total") >= 5000, "alto")
             .when(F.col("total") >= 1000, "medio")
             .otherwise("baixo"),
        )
        .select(
            "pedido_id", "data_venda", "ano", "mes", "cliente", "uf",
            "categoria", "quantidade", "preco", "total", "faixa_valor",
        )
    )

    resumo = (
        limpo.groupBy("uf", "mes", "categoria")
        .agg(
            F.count("*").alias("pedidos"),
            F.sum("quantidade").alias("itens"),
            F.sum("total").cast("decimal(18,2)").alias("receita"),
            F.round(F.avg("total"), 2).alias("ticket_medio"),
            F.countDistinct("cliente").alias("clientes"),
        )
        .orderBy("uf", "mes", "categoria")
    )

    return normalizado, limpo, resumo, rejeitados


# ---------------------------------------------------------------------------
# L de Load
# ---------------------------------------------------------------------------
def carregar(limpo: DataFrame, resumo: DataFrame, rejeitados: DataFrame) -> None:
    destino_fato = SAIDA / "vendas"
    destino_resumo = SAIDA / "resumo_mensal"
    destino_rejeitados = SAIDA / "rejeitados"

    inicio = time.perf_counter()
    (
        limpo
        # Cada particao em memoria grava um arquivo em CADA pasta uf=... .
        # Com `repartition("uf")` cada UF vira uma particao so, e sai um
        # arquivo por pasta. A funcao abaixo mede a diferenca.
        .repartition("uf")
        .write.mode("overwrite")          # overwrite e o que torna o ETL repetivel
        .partitionBy("uf")
        .parquet(str(destino_fato))
    )
    print(f"fato gravado em {time.perf_counter() - inicio:.1f}s -> {destino_fato}")

    # O resumo e pequeno: um arquivo so, e o consumidor le sem paginar.
    resumo.coalesce(1).write.mode("overwrite").parquet(str(destino_resumo))
    rejeitados.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitados)
    )

    for pasta in (destino_fato, destino_resumo, destino_rejeitados):
        # Ignorar `_SUCCESS` e os `.crc`: o Hadoop grava um checksum oculto
        # ao lado de cada arquivo de dados, e conta-los infla o numero.
        dados = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<16} {len(dados):>3} arquivo(s) de dados")


def demonstrar_arquivos_pequenos(limpo: DataFrame) -> None:
    """Mesma escrita, dois layouts de particao -- e o numero de arquivos muda."""
    destino = SAIDA / "_demo_particoes"
    for particoes, rotulo in ((4, "repartition(4)"), (None, "repartition('uf')")):
        origem = limpo.repartition(4) if particoes else limpo.repartition("uf")
        pasta = destino / rotulo.replace("(", "_").replace(")", "").replace("'", "")
        origem.write.mode("overwrite").partitionBy("uf").parquet(str(pasta))
        arquivos = list(pasta.rglob("*.parquet"))
        por_uf = sorted({p.parent.name for p in arquivos})
        print(f"{rotulo:<20} {len(arquivos):>3} arquivos em {len(por_uf)} pastas")
    # 4 particoes em memoria x 6 UFs = 24 arquivos; uma particao por UF = 6.
    # O CSV deste laboratorio cabe num bloco so, entao a entrada ja chega com
    # uma particao -- o efeito multiplicativo aparece assim que ela cresce.


# ---------------------------------------------------------------------------
# Verificacao -- ler de volta e provar que o que saiu e o que se esperava
# ---------------------------------------------------------------------------
def verificar(spark: SparkSession, linhas_esperadas: int) -> None:
    de_volta = spark.read.parquet(str(SAIDA / "vendas"))
    print("linhas de volta ..:", de_volta.count(), f"(esperado {linhas_esperadas})")
    print("colunas ..........:", de_volta.columns)
    # `uf` volta por ultimo e nao estava gravada dentro do arquivo: o Spark a
    # reconstroi a partir do nome da pasta. A coluna de particionamento deixa
    # de ser dado e vira metadado do caminho.

    inicio = time.perf_counter()
    total_geral = de_volta.count()
    tempo_total = time.perf_counter() - inicio

    inicio = time.perf_counter()
    total_sp = de_volta.filter(F.col("uf") == "SP").count()
    tempo_sp = time.perf_counter() - inicio

    print(f"leitura completa ..... {tempo_total * 1000:6.0f} ms -> {total_geral} linhas")
    print(f"filtro uf='SP' ....... {tempo_sp * 1000:6.0f} ms -> {total_sp} linhas")
    # O filtro nao le as outras UFs: o caminho ja diz o que tem dentro
    # (partition pruning). E por isso que a coluna de particionamento se
    # escolhe pelos filtros que a aplicacao vai fazer.

    spark.read.parquet(str(SAIDA / "resumo_mensal")).orderBy(
        F.desc("receita")
    ).show(5)


def main() -> None:
    spark = criar_sessao("aula03-03-etl-local")
    try:
        titulo("0. Fonte")
        gerar_csv_bruto()

        titulo("1. Extract -- ler o CSV bruto")
        bruto = extrair(spark)

        titulo("2. Transform -- normalizar, deduplicar, validar, enriquecer")
        normalizado, limpo, resumo, rejeitados = transformar(bruto)
        limpo.cache()
        total_bruto = bruto.count()
        total_normalizado = normalizado.count()
        total_limpo = limpo.count()
        total_rejeitado = rejeitados.count()
        print(f"lidas .............: {total_bruto}")
        print(f"apos dropDuplicates: {total_normalizado}"
              f"  (-{total_bruto - total_normalizado} duplicatas)")
        print(f"aprovadas .........: {total_limpo}")
        print(f"rejeitadas ........: {total_rejeitado}")
        # Conciliacao: toda linha que entrou tem de sair de um dos dois lados.
        # E a checagem mais barata de um ETL, e a que pega o erro mais comum --
        # a linha que some por causa de um NULL numa condicao.
        assert total_limpo + total_rejeitado == total_normalizado, (
            f"linhas perdidas: {total_normalizado - total_limpo - total_rejeitado}"
        )
        print("conciliacao .......: OK (aprovadas + rejeitadas = lidas apos dedup)")
        rejeitados.groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)
        limpo.show(5)

        titulo("3. Load -- gravar Parquet particionado por UF")
        carregar(limpo, resumo, rejeitados)

        titulo("4. O numero de arquivos e uma consequencia da particao")
        demonstrar_arquivos_pequenos(limpo)

        titulo("5. Verificacao -- ler de volta")
        verificar(spark, total_limpo)

        titulo("Resumo da execucao")
        print(f"entrada ......: {ARQUIVO_BRUTO}")
        print(f"lidas ........: {total_bruto}")
        print(f"aprovadas ....: {total_limpo}")
        print(f"rejeitadas ...: {total_rejeitado}")
        print(f"saida ........: {SAIDA}")
    finally:
        spark.stop()


def limpar_saida() -> None:
    """Apaga so a saida, preservando a entrada -- util para reexecutar do zero."""
    if SAIDA.exists():
        shutil.rmtree(SAIDA)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_saida()
        print(f"saida removida: {SAIDA}")
    main()
