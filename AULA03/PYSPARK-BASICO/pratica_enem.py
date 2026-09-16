"""Arquivo de trabalho dos exercicios 1 a 9 do EXERCICIOS_ENEM.md (niveis 1-3).

    python pratica_enem.py            roda o que estiver descomentado abaixo
    python pratica_enem.py 1          roda so o exercicio 1
    python pratica_enem.py 1 4 8      roda os exercicios 1, 4 e 8

Como usar
---------
1. Abra o EXERCICIOS_ENEM.md ao lado deste arquivo.
2. Escreva a sua resposta dentro da funcao `exercicio_N` correspondente.
3. Rode e compare com a "Resposta esperada" do enunciado.
4. So depois abra o bloco "Gabarito" do EXERCICIOS_ENEM.md.

O exercicio 1 ja vem resolvido, como modelo. Os outros tem o enunciado no
lugar do codigo -- e uma dica do que usar, sem entregar a resposta.

Os exercicios 10 a 14 (o ETL de verdade, com 8000 candidatos) ficam no
05_etl_enem_sc.py -- este arquivo aqui e so o aquecimento com uma tabela
pequena, do mesmo jeito que o pratica.py faz com `vendas`.
"""

import sys

from comum import criar_sessao, titulo

from pyspark.sql import Window
from pyspark.sql import functions as F

spark = criar_sessao("pratica-enem")

# ---------------------------------------------------------------------------
# O dado: oito inscricoes fictícias do ENEM em quatro cidades de SC
# ---------------------------------------------------------------------------
candidatos = spark.createDataFrame(
    [
        (1, "Ana",   "Florianopolis", "publica", 560.0, 640.0, "2024-01-15"),
        (2, "Bruno", "Joinville",     "privada", 610.0, 590.0, "2024-01-17"),
        (3, "Carla", "Blumenau",      "publica", 480.0, 520.0, "2024-02-03"),
        (4, "Ana",   "Florianopolis", "publica", 700.0, 680.0, "2024-02-11"),
        (5, "Diego", "Joinville",     "privada", 650.0, 610.0, "2024-02-20"),
        (6, "Carla", "Blumenau",      "publica", 540.0, 560.0, "2024-03-01"),
        (7, "Bruno", "Joinville",     "privada", 590.0, 610.0, "2024-03-05"),
        (8, "Elisa", "Florianopolis", "publica", 620.0, 600.0, None),  # sem data, de proposito
    ],
    "inscricao_id int, candidato string, municipio string, tipo_escola string, "
    "nota_lc double, nota_mt double, data_prova string",
)

candidatos = (
    candidatos
    .withColumn("media", (F.col("nota_lc") + F.col("nota_mt")) / 2)
    .withColumn("data_prova", F.to_date("data_prova", "yyyy-MM-dd"))
)

regioes = spark.createDataFrame(
    [
        ("Florianopolis", "Grande Florianopolis"),
        ("Joinville", "Norte Catarinense"),
        ("Blumenau", "Vale do Itajai"),
        ("Lages", "Serrana"),
    ],
    "municipio string, regiao string",
)


# ---------------------------------------------------------------------------
# Nivel 1 -- ler e filtrar
# ---------------------------------------------------------------------------
def exercicio_1():
    """Candidatos com media acima de 600, da maior para a menor.

    RESOLVIDO -- este e o modelo. Esperado: 3 candidatos (690, 630, 610).
    """
    (
        candidatos
        .filter(F.col("media") > 600)
        .select("inscricao_id", "candidato", "media")
        .orderBy(F.desc("media"))
        .show()
    )


def exercicio_2():
    """Quantas inscricoes sao de 'Joinville' E tipo_escola 'privada'?  Esperado: 3.

    Dica: .filter((cond1) & (cond2)).count() -- `and` do Python nao funciona.
    """
    print("escreva a sua resposta em exercicio_2()")


def exercicio_3():
    """Mostre a inscricao sem data_prova.  Esperado: a inscricao 8 (Elisa).

    Dica: F.col("data_prova").isNull()
    Depois teste: candidatos.filter(F.col("data_prova") != "2024-01-15").count()
    -- e explique por que o resultado nao e 7.
    """
    print("escreva a sua resposta em exercicio_3()")


# ---------------------------------------------------------------------------
# Nivel 2 -- agregar
# ---------------------------------------------------------------------------
def exercicio_4():
    """Media de nota_mt por tipo_escola, da maior para a menor.

    Esperado: privada 603.33, publica 600.00.
    Dica: groupBy + agg(F.round(F.avg(...), 2))
    """
    print("escreva a sua resposta em exercicio_4()")


def exercicio_5():
    """Inscricoes e candidatos DISTINTOS por tipo_escola.

    Esperado: publica 5/3, privada 3/2.
    Dica: F.count("*") e F.countDistinct("candidato") no mesmo agg.
    """
    print("escreva a sua resposta em exercicio_5()")


def exercicio_6():
    """A maior nota_mt de cada candidato.

    Esperado: Ana 680.0, Bruno 610.0, Diego 610.0, Elisa 600.0, Carla 560.0.
    Dica: groupBy("candidato") + F.max("nota_mt")
    """
    print("escreva a sua resposta em exercicio_6()")


# ---------------------------------------------------------------------------
# Nivel 3 -- juntar e janelar
# ---------------------------------------------------------------------------
def exercicio_7():
    """Soma da media por regiao, usando o DataFrame `regioes`.

    Esperado: Grande Florianopolis 1900.0, Norte Catarinense 1830.0,
    Vale do Itajai 1050.0 -- e nenhuma linha para Serrana (Lages nao aparece
    em `candidatos`).
    Dica: candidatos.join(regioes, on="municipio", how="left") -- `on` como string.
    """
    print("escreva a sua resposta em exercicio_7()")


def exercicio_8():
    """A LINHA INTEIRA da maior nota_mt de cada candidato (inscricao, municipio, nota_mt).

    Esperado: 5 linhas. Dica: Window.partitionBy("candidato").orderBy(F.desc("nota_mt"))
    + F.row_number().over(janela) + filter(posicao == 1).
    """
    print("escreva a sua resposta em exercicio_8()")


def exercicio_9():
    """Quanto cada inscricao representa da nota_mt total da sua tipo_escola, em %.

    Esperado: a inscricao 4 (Ana, publica, nota_mt 680) = 22.67%.
    Dica: F.sum("nota_mt").over(Window.partitionBy("tipo_escola")) numa coluna nova.
    """
    print("escreva a sua resposta em exercicio_9()")


# Os exercicios 10 a 14 constroem o 05_etl_enem_sc.py -- veja o EXERCICIOS_ENEM.md.

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
