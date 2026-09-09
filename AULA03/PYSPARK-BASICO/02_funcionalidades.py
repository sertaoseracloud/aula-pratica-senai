"""Exemplo 2 -- as funcionalidades do dia a dia sobre um DataFrame.

    python 02_funcionalidades.py

Selecao, filtro, coluna derivada, condicional, agregacao, join, window,
SQL, nulos e a comparacao entre funcao nativa e UDF em Python.

Oito linhas de venda, dez secoes. O dado e minusculo de proposito: aqui a
pergunta e "o que cada operacao faz", nao "quanto ela custa".

MAPA DAS SECOES
---------------
     1. select, alias, expressoes
     2. filter / where ............... e por que `and` do Python nao serve
     3. withColumn, cast, when ....... colunas derivadas e condicionais
     4. nulos ........................ o que some do seu filtro sem avisar
     5. groupBy e agregacoes ......... count, sum, avg, countDistinct
     6. join ......................... inclusive left_anti, para auditoria
     7. Window ....................... agregar SEM colapsar as linhas
     8. SQL .......................... a mesma coisa, escrita como SELECT
     9. particoes .................... repartition x coalesce, e o AQE
    10. nativa x UDF ................. 13x, e uma medicao que nao mede nada

O DADO
------
A linha 8 (Elisa) tem `data_venda` nula de proposito. Ela existe para que a
secao 4 mostre o que acontece com nulos em filtro -- nao e um descuido.
"""

import time

from comum import criar_sessao, titulo

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = criar_sessao("aula03-02-funcionalidades")

vendas = spark.createDataFrame(
    [
        (1, "Ana",   "SP", "eletronicos", 3, 1200.0, "2024-01-15"),
        (2, "Bruno", "RJ", "livros",      1,   45.9, "2024-01-17"),
        (3, "Carla", "MG", "eletronicos", 2,  800.0, "2024-02-03"),
        (4, "Ana",   "SP", "livros",      5,   30.0, "2024-02-11"),
        (5, "Diego", "RJ", "moveis",      1, 2300.0, "2024-02-20"),
        (6, "Carla", "MG", "moveis",      2, 1150.0, "2024-03-01"),
        (7, "Bruno", "RJ", "eletronicos", 1,  950.0, "2024-03-05"),
        (8, "Elisa", "SP", "livros",      4,   60.0, None),
    ],
    "pedido_id int, cliente string, uf string, categoria string, "
    "quantidade int, preco double, data_venda string",
)

# ---------------------------------------------------------------------------
titulo("1. select, alias e expressoes")
# ---------------------------------------------------------------------------
vendas.select("pedido_id", "cliente", F.col("preco").alias("preco_unitario")).show(3)
vendas.selectExpr("pedido_id", "quantidade * preco as total").show(3)

# ---------------------------------------------------------------------------
titulo("2. filter / where -- as duas sao a mesma funcao")
# ---------------------------------------------------------------------------
vendas.filter((F.col("uf") == "SP") & (F.col("preco") > 100)).show()
vendas.where("categoria = 'livros'").show()
# `&` e `|` no lugar de `and`/`or`, e cada condicao entre parenteses:
# `and` em Python avalia o objeto Column como booleano e levanta erro.

# ---------------------------------------------------------------------------
titulo("3. withColumn, cast e when/otherwise")
# ---------------------------------------------------------------------------
enriquecido = (
    vendas
    .withColumn("total", F.col("quantidade") * F.col("preco"))
    .withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))
    .withColumn("mes", F.month("data_venda"))
    .withColumn(
        "faixa",
        F.when(F.col("preco") >= 1000, "alto")
         .when(F.col("preco") >= 100, "medio")
         .otherwise("baixo"),
    )
)
enriquecido.select("pedido_id", "total", "mes", "faixa").show()

# ---------------------------------------------------------------------------
titulo("4. Nulos -- o que acontece com a linha 8 (data_venda ausente)")
# ---------------------------------------------------------------------------
enriquecido.filter(F.col("data_venda").isNull()).show()
print("linhas com mes nulo:", enriquecido.filter(F.col("mes").isNull()).count())
enriquecido.fillna({"mes": 0}).select("pedido_id", "mes").show(8)
print("apos dropna(subset=data_venda):", enriquecido.dropna(subset=["data_venda"]).count())
# Nulo nao e zero e nao e string vazia: qualquer comparacao com ele da nulo,
# e nao falso. Por isso `col != 'x'` descarta silenciosamente as linhas nulas.

# ---------------------------------------------------------------------------
titulo("5. groupBy e agregacoes")
# ---------------------------------------------------------------------------
(
    enriquecido.groupBy("uf")
    .agg(
        F.count("*").alias("pedidos"),
        F.sum("total").cast("decimal(18,2)").alias("receita"),
        F.round(F.avg("total"), 2).alias("ticket_medio"),
        F.countDistinct("cliente").alias("clientes"),
    )
    .orderBy(F.desc("receita"))
    .show()
)
# `sum` de double imprime em notacao cientifica assim que a base cresce.
# O cast para decimal resolve -- e e o mesmo motivo pelo qual dinheiro nao
# se guarda em ponto flutuante.

# ---------------------------------------------------------------------------
titulo("6. join")
# ---------------------------------------------------------------------------
regioes = spark.createDataFrame(
    [("SP", "Sudeste"), ("RJ", "Sudeste"), ("MG", "Sudeste"), ("BA", "Nordeste")],
    "uf string, regiao string",
)
enriquecido.join(regioes, on="uf", how="left").select("pedido_id", "uf", "regiao").show()
print("anti join (UFs sem venda):")
regioes.join(enriquecido, on="uf", how="left_anti").show()
# `left_anti` responde "o que existe de um lado e nao do outro" sem trazer
# coluna nenhuma do outro lado -- e a forma barata de auditar cobertura.

# ---------------------------------------------------------------------------
titulo("7. Window -- ranking e acumulado sem colapsar as linhas")
# ---------------------------------------------------------------------------
janela = Window.partitionBy("uf").orderBy(F.desc("total"))
(
    enriquecido
    .withColumn("posicao_na_uf", F.row_number().over(janela))
    .withColumn("total_da_uf", F.sum("total").over(Window.partitionBy("uf")))
    .select("uf", "cliente", "total", "posicao_na_uf", "total_da_uf")
    .orderBy("uf", "posicao_na_uf")
    .show()
)
# groupBy reduz N linhas a uma; window mantem as N e anexa o agregado.

# ---------------------------------------------------------------------------
titulo("8. SQL sobre o mesmo DataFrame")
# ---------------------------------------------------------------------------
enriquecido.createOrReplaceTempView("vendas")
spark.sql(
    """
    SELECT categoria,
           COUNT(*)                          AS pedidos,
           CAST(SUM(total) AS DECIMAL(18,2)) AS receita
    FROM vendas
    GROUP BY categoria
    ORDER BY receita DESC
    """
).show()
# Mesma API por baixo: DataFrame e SQL viram o mesmo plano no Catalyst.

# ---------------------------------------------------------------------------
titulo("9. Particoes: o numero decide o paralelismo e o numero de arquivos")
# ---------------------------------------------------------------------------
print("particoes originais .....:", enriquecido.rdd.getNumPartitions())
print("apos repartition(2) .....:", enriquecido.repartition(2).rdd.getNumPartitions())
print("apos coalesce(1) ........:", enriquecido.coalesce(1).rdd.getNumPartitions())
por_particao = enriquecido.repartition("uf").rdd.glom().map(len).collect()
print("linhas por particao apos repartition('uf'):", por_particao)
# `repartition` embaralha (shuffle) e pode aumentar; `coalesce` so junta,
# sem shuffle, e por isso so diminui.
#
# A ultima linha imprime `[8]` -- uma particao so, com as oito linhas. Foram
# criadas tres (uma por UF), e o AQE (adaptive query execution, ligado por
# padrao no Spark 3) juntou as tres por serem minusculas. Em volume real elas
# permanecem separadas; aqui o proprio Spark decidiu que o shuffle nao valia.

# ---------------------------------------------------------------------------
titulo("10. Funcao nativa x UDF em Python")
# ---------------------------------------------------------------------------
base = spark.range(0, 200_000).withColumn("texto", F.concat(F.lit("cliente_"), F.col("id")))
base.cache().count()          # tira o custo de gerar o dado da medicao

maiuscula_udf = F.udf(lambda s: s.upper() if s else None, StringType())


def medir(nome, df_resultado):
    t = time.perf_counter()
    df_resultado.agg(F.max("saida")).collect()   # a acao consome a coluna
    print(f"{nome:<24} {(time.perf_counter() - t) * 1000:8.0f} ms")


medir("nativa F.upper()", base.withColumn("saida", F.upper("texto")))
medir("UDF Python", base.withColumn("saida", maiuscula_udf("texto")))

t = time.perf_counter()
base.withColumn("saida", maiuscula_udf("texto")).count()
print(f"{'UDF + count()':<24} {(time.perf_counter() - t) * 1000:8.0f} ms  <- nao mediu nada")
# `count()` nao usa a coluna calculada: o otimizador apaga a projecao inteira
# do plano e a UDF nunca roda. Para medir uma transformacao, a acao precisa
# consumir o resultado dela.
base.unpersist()

spark.stop()
