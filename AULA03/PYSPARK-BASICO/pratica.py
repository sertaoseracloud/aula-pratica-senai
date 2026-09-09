"""Arquivo de trabalho dos exercicios do EXERCICIOS.md.

    python pratica.py            roda o que estiver descomentado abaixo
    python pratica.py 1          roda so o exercicio 1
    python pratica.py 1 4 8      roda os exercicios 1, 4 e 8

Como usar
---------
1. Abra o EXERCICIOS.md ao lado deste arquivo.
2. Escreva a sua resposta dentro da funcao `exercicio_N` correspondente.
3. Rode e compare com a "Resposta esperada" do enunciado.
4. So depois abra o bloco "Gabarito" do EXERCICIOS.md.

O exercicio 1 ja vem resolvido, como modelo. Os outros tem o enunciado no
lugar do codigo -- e uma dica do que usar, sem entregar a resposta.

Este arquivo e seu: risque, quebre, experimente. Nada aqui e usado pelos
outros scripts do laboratorio.
"""

import sys

from comum import criar_sessao, titulo

from pyspark.sql import Window
from pyspark.sql import functions as F

spark = criar_sessao("pratica")

# ---------------------------------------------------------------------------
# O dado: as mesmas oito vendas do 02_funcionalidades.py
# ---------------------------------------------------------------------------
vendas = spark.createDataFrame(
    [
        (1, "Ana",   "SP", "eletronicos", 3, 1200.0, "2024-01-15"),
        (2, "Bruno", "RJ", "livros",      1,   45.9, "2024-01-17"),
        (3, "Carla", "MG", "eletronicos", 2,  800.0, "2024-02-03"),
        (4, "Ana",   "SP", "livros",      5,   30.0, "2024-02-11"),
        (5, "Diego", "RJ", "moveis",      1, 2300.0, "2024-02-20"),
        (6, "Carla", "MG", "moveis",      2, 1150.0, "2024-03-01"),
        (7, "Bruno", "RJ", "eletronicos", 1,  950.0, "2024-03-05"),
        (8, "Elisa", "SP", "livros",      4,   60.0, None),    # sem data, de proposito
    ],
    "pedido_id int, cliente string, uf string, categoria string, "
    "quantidade int, preco double, data_venda string",
)

vendas = (
    vendas
    .withColumn("total", F.col("quantidade") * F.col("preco"))
    .withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))
)

regioes = spark.createDataFrame(
    [("SP", "Sudeste"), ("RJ", "Sudeste"), ("MG", "Sudeste"), ("BA", "Nordeste")],
    "uf string, regiao string",
)


# ---------------------------------------------------------------------------
# Nivel 1 -- ler e filtrar
# ---------------------------------------------------------------------------
def exercicio_1():
    """Pedidos com total acima de 1000, do maior para o menor.

    RESOLVIDO -- este e o modelo. Esperado: 4 pedidos (3600, 2300, 2300, 1600).
    """
    (
        vendas
        .filter(F.col("total") > 1000)
        .select("pedido_id", "cliente", "total")
        .orderBy(F.desc("total"))
        .show()
    )


def exercicio_2():
    """Quantos pedidos sao de 'livros' E da UF 'SP'?  Esperado: 2.

    Dica: .filter((cond1) & (cond2)).count() -- `and` do Python nao funciona.
    """
    print("escreva a sua resposta em exercicio_2()")


def exercicio_3():
    """Mostre o pedido sem data_venda.  Esperado: o pedido 8.

    Dica: F.col("data_venda").isNull()
    Depois teste: vendas.filter(F.col("data_venda") != "2024-01-15").count()
    -- e explique por que o resultado nao e 7.
    """
    print("escreva a sua resposta em exercicio_3()")


# ---------------------------------------------------------------------------
# Nivel 2 -- agregar
# ---------------------------------------------------------------------------
def exercicio_4():
    """Receita por categoria, da maior para a menor.

    Esperado: eletronicos 6150.00, moveis 4600.00, livros 435.90.
    Dica: groupBy + agg(F.sum(...).cast("decimal(18,2)"))
    """
    print("escreva a sua resposta em exercicio_4()")


def exercicio_5():
    """Pedidos e clientes DISTINTOS por UF.  Esperado: SP 3/2, RJ 3/2, MG 2/1.

    Dica: F.count("*") e F.countDistinct("cliente") no mesmo agg.
    """
    print("escreva a sua resposta em exercicio_5()")


def exercicio_6():
    """O valor do maior pedido de cada cliente.

    Esperado: Ana 3600, Carla 2300, Diego 2300, Bruno 950, Elisa 240.
    Dica: groupBy("cliente") + F.max("total")
    """
    print("escreva a sua resposta em exercicio_6()")


# ---------------------------------------------------------------------------
# Nivel 3 -- juntar e janelar
# ---------------------------------------------------------------------------
def exercicio_7():
    """Receita por regiao, usando o DataFrame `regioes`.  Esperado: Sudeste 11185.90.

    Dica: vendas.join(regioes, on="uf", how="left") -- `on` como string.
    """
    print("escreva a sua resposta em exercicio_7()")


def exercicio_8():
    """A LINHA INTEIRA do maior pedido de cada cliente (id, categoria, total).

    Esperado: 5 linhas. Dica: Window.partitionBy("cliente").orderBy(F.desc("total"))
    + F.row_number().over(janela) + filter(posicao == 1).
    """
    print("escreva a sua resposta em exercicio_8()")


def exercicio_9():
    """Quanto cada pedido representa da receita da sua UF, em %.

    Esperado: o pedido 1 (Ana, SP) = 90.23%.
    Dica: F.sum("total").over(Window.partitionBy("uf")) numa coluna nova.
    """
    print("escreva a sua resposta em exercicio_9()")


# Os exercicios 10 a 12 alteram o 03_etl_local.py -- veja o EXERCICIOS.md.

EXERCICIOS = {
    1: exercicio_1,
    2: exercicio_2,
    3: exercicio_3,
    4: exercicio_4,
    5: exercicio_5,
    6: exercicio_6,
    7: exercicio_7,
    8: exercicio_8,
    9: exercicio_9,
}

# Argumentos da linha de comando, ou todos.
pedidos = [int(a) for a in sys.argv[1:] if a.isdigit()] or sorted(EXERCICIOS)

for numero in pedidos:
    funcao = EXERCICIOS.get(numero)
    if funcao is None:
        print(f"exercicio {numero} nao existe neste arquivo (1 a 9)")
        continue
    titulo(f"Exercicio {numero}")
    # A primeira linha da docstring e o enunciado resumido.
    print((funcao.__doc__ or "").strip().splitlines()[0], "\n")
    funcao()

spark.stop()
