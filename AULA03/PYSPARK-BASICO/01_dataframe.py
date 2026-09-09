"""Exemplo 1 -- as maneiras de criar um DataFrame, e o que cada uma decide.

    python 01_dataframe.py

Um DataFrame e uma tabela distribuida com esquema. As formas de criar mudam
em uma coisa central: quem define os tipos -- voce ou o Spark.

Rode `00_primeiro_contato.py` antes deste, se ainda nao rodou.

MAPA DAS SECOES
---------------
    1. lista de tuplas + nomes ...... o Spark infere os tipos
    2. schema em string DDL ......... voce declara, em uma linha
    3. StructType explicito ......... voce declara, com nulabilidade
    4. Row e dicionarios ............ quando o dado ja vem assim
    5. spark.range .................. gerar volume para medir
    6. a partir de um pandas ........ trazer dado pequeno de fora
    7. inspecionar .................. o que olhar antes de transformar
    8. transformacao x acao ......... a distincao que explica tudo
    9. o plano ...................... o que o Spark vai executar de fato

SOBRE TIPOS
-----------
O Spark tem o seu proprio sistema de tipos, e ele NAO e o do Python. Um `1`
escrito em Python vira `bigint` (LongType) do lado da JVM, uma string vira
`string` (StringType), um `float` vira `double`. A conversao acontece na
fronteira entre os dois mundos, e e por isso que `IntegerType` e `LongType`
sao coisas diferentes la dentro -- em Python, `int` e so `int`.
"""

from comum import criar_sessao, titulo

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = criar_sessao("aula03-01-dataframe")

# ---------------------------------------------------------------------------
titulo("1. Lista de tuplas + nomes de coluna (o jeito mais curto)")
# ---------------------------------------------------------------------------
df = spark.createDataFrame(
    [(1, "Ana", "SP", 2500.0), (2, "Bruno", "RJ", 1800.5), (3, "Carla", "MG", 3200.0)],
    ["id", "nome", "uf", "valor"],
)
df.show()
df.printSchema()
# O Spark inferiu os tipos a partir dos valores Python. Rapido de escrever,
# mas quem manda no tipo e o dado -- e um `None` na primeira linha ja muda
# a inferencia.

# ---------------------------------------------------------------------------
titulo("2. Schema DDL em string -- tipos declarados, sem importar types")
# ---------------------------------------------------------------------------
df_ddl = spark.createDataFrame(
    [(1, "Ana", "SP", 2500.0), (2, "Bruno", "RJ", 1800.5)],
    "id int, nome string, uf string, valor double",
)
print(df_ddl.dtypes)

# ---------------------------------------------------------------------------
titulo("3. StructType explicito -- o unico que controla nulabilidade")
# ---------------------------------------------------------------------------
schema = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("nome", StringType(), nullable=False),
        StructField("uf", StringType(), nullable=True),
        StructField("valor", DoubleType(), nullable=True),
    ]
)
df_schema = spark.createDataFrame(
    [(1, "Ana", "SP", 2500.0), (2, "Bruno", None, None)], schema=schema
)
df_schema.printSchema()
df_schema.show()
# `uf` e `valor` aceitam nulo; `id` e `nome` nao. Declarar o schema tambem
# evita que o Spark leia o dado so para descobrir os tipos -- em arquivo
# grande, a inferencia e uma passada inteira a mais.

# ---------------------------------------------------------------------------
titulo("4. Row e dicionarios")
# ---------------------------------------------------------------------------
Pedido = Row("id", "produto", "quantidade")
df_row = spark.createDataFrame([Pedido(1, "teclado", 2), Pedido(2, "monitor", 1)])
df_row.show()

df_dict = spark.createDataFrame([{"id": 1, "produto": "teclado"}, {"id": 2, "produto": "mouse"}])
df_dict.show()

# ---------------------------------------------------------------------------
titulo("5. spark.range -- gerador embutido, util para volume")
# ---------------------------------------------------------------------------
df_range = spark.range(0, 1_000_000, numPartitions=8)
print("linhas:", df_range.count(), "| particoes:", df_range.rdd.getNumPartitions())
# `range` ja nasce particionado. Repare que o numero de particoes e uma
# escolha sua, e nao uma consequencia do tamanho do dado.

# ---------------------------------------------------------------------------
titulo("6. A partir de um DataFrame pandas")
# ---------------------------------------------------------------------------
try:
    import pandas as pd

    pdf = pd.DataFrame({"id": [1, 2, 3], "cidade": ["Recife", "Salvador", "Curitiba"]})
    spark.createDataFrame(pdf).show()
except ImportError:
    print("pandas nao instalado -- opcional neste laboratorio")

# ---------------------------------------------------------------------------
titulo("7. Inspecionar: o que olhar antes de transformar")
# ---------------------------------------------------------------------------
print("colunas .....:", df.columns)
print("tipos .......:", df.dtypes)
print("linhas ......:", df.count())
print("particoes ...:", df.rdd.getNumPartitions())
df.describe().show()          # contagem, media, desvio, min e max
df.select("uf").distinct().show()

# ---------------------------------------------------------------------------
titulo("8. Transformacao x acao -- a diferenca que explica tudo")
# ---------------------------------------------------------------------------
transformacao = df.filter(F.col("valor") > 2000)     # nada rodou ainda
print("apos o filter: nenhum job foi disparado")
print("apos o collect:", transformacao.collect())    # agora sim
# `filter`, `select` e `withColumn` sao preguicosos: constroem um plano.
# `show`, `count`, `collect` e `write` sao acoes: disparam a execucao.
# Por isso um erro de coluna inexistente as vezes so aparece na acao.

titulo("9. O plano que o Spark realmente vai executar")
transformacao.explain()

spark.stop()
