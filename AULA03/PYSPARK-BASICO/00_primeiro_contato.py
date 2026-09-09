"""Exemplo 0 -- o menor programa PySpark possivel, linha por linha.

    python 00_primeiro_contato.py

Se voce nunca abriu o Spark, comece por aqui. Sao vinte linhas uteis, e cada
uma delas e explicada. Os outros tres scripts assumem que este rodou.

O QUE E O SPARK, EM UM PARAGRAFO
--------------------------------
Spark e um motor que executa o mesmo calculo sobre pedacos diferentes do dado
ao mesmo tempo. Voce escreve o programa uma vez; ele divide o dado em
PARTICOES e roda uma TAREFA por particao. Neste laboratorio tudo acontece
dentro da sua maquina (`local[4]` = quatro tarefas em paralelo), mas o codigo
e exatamente o mesmo que roda num cluster de cem maquinas -- e essa e a razao
de aprender a API assim, sem cluster nenhum.

QUEM E QUEM
-----------
    driver     o seu processo Python. Monta o plano e coordena. E aqui que
               `print` imprime e onde o `collect()` deposita o resultado.
    executor   quem faz o trabalho. Em `local[4]`, sao threads do mesmo
               processo; num cluster, outras maquinas.
    particao   um pedaco do dado. Uma tarefa por particao, sempre.
    JVM        o Spark e escrito em Scala e roda em Java. O PySpark conversa
               com essa JVM por um "gateway" -- e por isso Java e obrigatorio,
               e por isso a JVM leva ~5 s para subir em cada execucao.
"""

from comum import criar_sessao, titulo

# `functions` traz as funcoes de coluna do Spark (upper, sum, when, ...).
# O apelido `F` e convencao universal em PySpark: voce vai ver `F.col(...)`
# em todo codigo do mundo real, entao vale acostumar desde agora.
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1. A sessao -- o ponto de entrada de tudo
# ---------------------------------------------------------------------------
# `criar_sessao` (em comum.py) e s uma casca em volta de SparkSession.builder.
# Criar a sessao sobe a JVM: e o passo lento de qualquer script Spark, ~5 s
# aqui. Num script de producao voce cria UMA sessao e a reaproveita.
spark = criar_sessao("aula03-00-primeiro-contato")

titulo("1. A sessao esta de pe")
print("versao do Spark ..:", spark.version)
print("master ...........:", spark.sparkContext.master)   # local[4]
print("aplicacao ........:", spark.sparkContext.appName)

# ---------------------------------------------------------------------------
# 2. O primeiro DataFrame
# ---------------------------------------------------------------------------
# Um DataFrame e uma tabela com esquema (nomes e tipos de coluna), dividida em
# particoes. Ele NAO guarda os dados na memoria do seu Python -- guarda a
# receita de como obte-los.
titulo("2. Um DataFrame de tres linhas")
pessoas = spark.createDataFrame(
    [("Ana", 34, "SP"), ("Bruno", 28, "RJ"), ("Carla", 45, "MG")],
    ["nome", "idade", "uf"],          # nomes das colunas, na ordem das tuplas
)

pessoas.show()                        # imprime a tabela
pessoas.printSchema()                 # imprime nomes e tipos

# ---------------------------------------------------------------------------
# 3. Uma transformacao -- e a surpresa de que nada rodou
# ---------------------------------------------------------------------------
titulo("3. Transformacao: o Spark anota, mas nao executa")
adultos = pessoas.filter(F.col("idade") >= 30)
print("o filter voltou instantaneamente e NAO leu nenhuma linha.")
print("o que existe agora e um plano:", type(adultos).__name__)

# `F.col("idade") >= 30` nao devolve True nem False: devolve um OBJETO Column
# que descreve a comparacao. Por isso `and`/`or`/`not` do Python nao funcionam
# em condicoes de DataFrame -- use `&`, `|` e `~`, com parenteses:
#     df.filter((F.col("a") > 1) & (F.col("b") == "x"))

# ---------------------------------------------------------------------------
# 4. Uma acao -- agora sim
# ---------------------------------------------------------------------------
titulo("4. Acao: a execucao acontece aqui")
adultos.show()
print("quantidade:", adultos.count())

# TRANSFORMACAO x ACAO -- a distincao mais importante do Spark:
#
#   transformacoes  select, filter, withColumn, groupBy, join, orderBy...
#                   Devolvem um novo DataFrame e nao executam nada.
#   acoes           show, count, collect, take, write, toPandas...
#                   Disparam a execucao do plano acumulado ate ali.
#
# Consequencia pratica: um erro na sua transformacao (coluna inexistente,
# divisao por zero) as vezes so estoura muitas linhas depois, na acao. Ao
# depurar, procure a ACAO que falhou e leia o plano dela, nao so a linha
# apontada no traceback.

# ---------------------------------------------------------------------------
# 5. Uma coluna nova
# ---------------------------------------------------------------------------
titulo("5. withColumn cria uma coluna a partir das outras")
com_faixa = pessoas.withColumn(
    "faixa",
    F.when(F.col("idade") >= 40, "40+")
     .when(F.col("idade") >= 30, "30-39")
     .otherwise("ate 29"),
)
com_faixa.show()
# `withColumn` NAO altera `pessoas`: DataFrame e imutavel. Ele devolve um novo
# DataFrame, e por isso o resultado precisa ser atribuido a alguma variavel.
print("pessoas continua com as colunas originais:", pessoas.columns)

# ---------------------------------------------------------------------------
# 6. Trazer o resultado para o Python
# ---------------------------------------------------------------------------
titulo("6. collect() traz os dados para a memoria do driver")
linhas = com_faixa.collect()          # lista de objetos Row
for linha in linhas:
    print(f"  {linha['nome']:<6} {linha['idade']:>3} anos  ({linha['faixa']})")

# CUIDADO: `collect()` traz TUDO para a memoria do seu processo Python. Com
# tres linhas e inofensivo; com dez milhoes, derruba o driver com OutOfMemory.
# Para espiar o conteudo, use `show(5)` ou `take(5)`, que trazem so o comeco.
print("as primeiras 2 linhas, sem trazer o resto:", com_faixa.take(2))

# ---------------------------------------------------------------------------
# 7. Encerrar
# ---------------------------------------------------------------------------
# `stop()` derruba a JVM e libera as portas. Sem ele o processo pode ficar
# pendurado ao fim do script.
spark.stop()

titulo("Foi isso")
print(
    "Voce criou uma sessao, montou um DataFrame, transformou, executou\n"
    "e trouxe o resultado de volta. O proximo passo e 01_dataframe.py."
)
