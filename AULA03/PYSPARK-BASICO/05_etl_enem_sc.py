"""Exercicio de ETL -- notas do ENEM (simuladas) em dez cidades de SC.

    python 05_etl_enem_sc.py

Este arquivo E o exercicio: ele te da a Fonte (gerador do CSV), o Extract e o
Load prontos, mas deixa cinco blocos de Transform com `# TODO Exercicio N`
no lugar do codigo. Leia o enunciado de cada um no EXERCICIOS_ENEM.md
(nivel 4), escreva a linha que falta e rode de novo.

Enquanto um TODO nao for preenchido, a funcao devolve `None` e o script para
com um erro claro (`AttributeError: 'NoneType' object has no attribute ...`)
apontando exatamente qual bloco falta -- e proposital, e o mesmo tipo de
sinal que um pipeline real da quando uma etapa nao roda.

O dado NAO e ENEM de verdade: e gerado com semente fixa (linha abaixo),
so para o exercicio ter numeros para comparar. As cidades e a geografia sao
reais; as notas, os candidatos e as inconsistencias sao sinteticas.

    dados/enem_sc_bruto.csv     entrada (gerada na primeira execucao, 8000 linhas)
    dados/municipios_sc.csv     tabela de apoio (municipio -> regiao)
    saida/enem/municipio=.../   fato limpo, particionado por municipio
    saida/resumo_regiao/        media geral por regiao
    saida/municipios_destaque/  municipios com 700+ presentes aprovados
    saida/enem_rejeitados/      linhas que nao passaram na validacao
"""

import csv
import random
import shutil

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

ARQUIVO_ENEM = DADOS / "enem_sc_bruto.csv"
ARQUIVO_MUNICIPIOS = DADOS / "municipios_sc.csv"

MUNICIPIOS_REGIOES = [
    ("Florianopolis", "Grande Florianopolis", 522000),
    ("Sao Jose", "Grande Florianopolis", 254000),
    ("Joinville", "Norte Catarinense", 604000),
    ("Jaragua do Sul", "Norte Catarinense", 190000),
    ("Blumenau", "Vale do Itajai", 361000),
    ("Itajai", "Vale do Itajai", 224000),
    ("Balneario Camboriu", "Vale do Itajai", 145000),
    ("Chapeco", "Oeste Catarinense", 226000),
    ("Criciuma", "Sul Catarinense", 217000),
    ("Lages", "Serrana", 158000),
]
NOMES_MUNICIPIOS = [m for m, _, _ in MUNICIPIOS_REGIOES]
ANOS_VALIDOS = [2022, 2023, 2024]

# Schema declarado: sem ele o Spark faz uma passada so para inferir tipos, e
# uma unica linha suja transforma a coluna inteira em string.
SCHEMA_ENEM = StructType(
    [
        StructField("inscricao_id", IntegerType(), True),
        StructField("ano", IntegerType(), True),
        StructField("municipio", StringType(), True),
        StructField("tipo_escola", StringType(), True),
        StructField("presente", StringType(), True),
        StructField("nota_lc", DoubleType(), True),
        StructField("nota_ch", DoubleType(), True),
        StructField("nota_cn", DoubleType(), True),
        StructField("nota_mt", DoubleType(), True),
        StructField("nota_redacao", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- gera os dois CSVs uma unica vez, com a sujeira que um ETL encontra
# ---------------------------------------------------------------------------
def gerar_csv_enem(linhas: int = 8_000) -> None:
    if ARQUIVO_ENEM.exists():
        print(f"entrada ja existe: {ARQUIVO_ENEM} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(42)          # semente fixa: numeros reproduziveis
    cabecalho = [
        "inscricao_id", "ano", "municipio", "tipo_escola", "presente",
        "nota_lc", "nota_ch", "nota_cn", "nota_mt", "nota_redacao",
    ]
    with ARQUIVO_ENEM.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        for inscricao_id in range(1, linhas + 1):
            ano = aleatorio.choice(ANOS_VALIDOS)
            municipio = aleatorio.choice(NOMES_MUNICIPIOS)
            tipo_escola = aleatorio.choices(
                ["publica", "privada", "nao_informada"], weights=[60, 30, 10]
            )[0]
            presente = aleatorio.choices(["S", "N"], weights=[90, 10])[0]

            if presente == "S":
                media, desvio = (580, 70) if tipo_escola == "privada" else (520, 80)
                notas = [
                    round(max(0.0, min(1000.0, aleatorio.gauss(media, desvio))), 1)
                    for _ in range(4)
                ]
                redacao = round(max(0.0, min(1000.0, aleatorio.gauss(media + 20, 90))), 1)
            else:
                notas = ["", "", "", ""]
                redacao = ""

            linha = [inscricao_id, ano, municipio, tipo_escola, presente, *notas, redacao]

            # ~5% de sujeira, dos tipos que aparecem de verdade em base real
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[2] = ""                       # municipio ausente
            elif sorteio < 0.02:
                linha[2] = linha[2].lower()          # municipio em formato errado
            elif sorteio < 0.03:
                linha[8] = 1050.0 if aleatorio.random() < 0.5 else -30.0  # nota fora da faixa
            elif sorteio < 0.04:
                linha[4] = "S"
                linha[5] = ""                        # presente mas sem nota_lc
            elif sorteio < 0.05:
                linha[1] = 2021                      # ano fora do conjunto valido
            escritor.writerow(linha)

            if sorteio < 0.004:                      # duplicata exata
                escritor.writerow(linha)

    tamanho = ARQUIVO_ENEM.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_ENEM} ({tamanho:.0f} KB)")


def gerar_csv_municipios() -> None:
    if ARQUIVO_MUNICIPIOS.exists():
        print(f"entrada ja existe: {ARQUIVO_MUNICIPIOS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    with ARQUIVO_MUNICIPIOS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(["municipio", "regiao", "populacao_estimada"])
        for municipio, regiao, populacao in MUNICIPIOS_REGIOES:
            escritor.writerow([municipio.upper(), regiao, populacao])

    print(f"entrada gerada: {ARQUIVO_MUNICIPIOS}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: ler bem um CSV e boilerplate
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_ENEM)
        .csv(str(ARQUIVO_ENEM))
    )
    municipios = spark.read.option("header", "true").option("inferSchema", "true").csv(
        str(ARQUIVO_MUNICIPIOS)
    )
    print("linhas lidas (enem) .....:", bruto.count())
    print("linhas lidas (municipios):", municipios.count())
    bruto.show(5, truncate=False)
    return bruto, municipios


# ---------------------------------------------------------------------------
# T de Transform -- cinco blocos, cinco exercicios
# ---------------------------------------------------------------------------
def normalizar(bruto: DataFrame) -> DataFrame:
    """Exercicio 10 -- padronizar municipio e tirar duplicatas exatas.

    Duas coisas, nesta ordem:
    1. `municipio` para maiusculo e sem espaco nas pontas (mesmo padrao do
       `uf` no 03_etl_local.py: F.upper(F.trim(...))).
    2. dropDuplicates nas colunas que identificam uma inscricao unica:
       inscricao_id, ano, municipio, tipo_escola, presente.

    Resposta esperada: 8000 linhas (36 duplicatas removidas de 8036 lidas).
    """
    # TODO Exercicio 10: normalizar municipio e remover duplicatas.
    return None


def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Exercicio 11 -- separar quem passa na validacao de quem nao passa.

    Uma linha e valida quando, ao mesmo tempo:
    - `municipio` esta na lista NOMES_MUNICIPIOS (compare em maiusculo);
    - `ano` esta em ANOS_VALIDOS;
    - se `presente == "S"`, `nota_lc` nao pode ser nula;
    - nenhuma das cinco notas (lc, ch, cn, mt, redacao) pode estar fora de
      [0, 1000] -- mas uma nota nula (presente == "N") NAO conta como erro.

    Junte as quatro condicoes com `&` dentro de um F.coalesce(..., F.lit(False))
    -- exatamente como a `regra_valida` do 03_etl_local.py. Sem o coalesce,
    uma condicao que vira NULL (por exemplo `presente == "S"` avaliado numa
    linha com presente nulo) faz a linha inteira sumir dos dois lados.

    Monte tambem a coluna `motivo` nos rejeitados, com um F.when/.otherwise
    por regra -- veja o `rejeitados` do 03_etl_local.py como modelo.

    Resposta esperada: 7659 aprovadas, 341 rejeitadas. Motivos: ano invalido
    91, nota fora da faixa 0-1000 89, municipio invalida ou ausente 89,
    notas ausentes para presente 72.
    """
    # TODO Exercicio 11: regra_valida = F.coalesce(..., F.lit(False))
    regra_valida = None

    rejeitados = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.lit(None).cast("string"),  # TODO Exercicio 11: cadeia de F.when(...).otherwise(...)
    )
    aprovados = normalizado.filter(regra_valida)
    return aprovados, rejeitados


def enriquecer(aprovados: DataFrame) -> DataFrame:
    """Exercicio 12 -- media geral e faixa de desempenho.

    Acrescente duas colunas:
    - `media_geral`: a media aritmetica das cinco notas. Some as cinco
      colunas e divida por 5 -- se QUALQUER uma for nula (candidato que nao
      compareceu), o resultado ja sai nulo sozinho, sem precisar de F.when.
      Arredonde com F.round(..., 2).
    - `faixa_desempenho`: "nao_compareceu" quando `presente == "N"`,
      "baixo" quando media_geral < 500, "medio" quando <= 650, "alto" acima
      disso. Uma cadeia de F.when cobre os quatro casos.

    Resposta esperada, contando so quem compareceu: baixo 1292, medio 5576,
    alto 43 (total 6911 -- bate com os presentes aprovados do exercicio 11).
    """
    # TODO Exercicio 12: withColumn("media_geral", ...) + withColumn("faixa_desempenho", ...)
    return None


def resumir_por_regiao(limpo: DataFrame, municipios: DataFrame) -> DataFrame:
    """Exercicio 13 -- media geral por regiao, juntando com `municipios`.

    Junte `limpo` com `municipios` por `municipio` (on como string, como no
    exercicio 7 do join de vendas/regioes), agrupe por `regiao` e calcule
    F.round(F.avg("media_geral"), 2) -- o AVG do Spark ja ignora nulos
    sozinho, entao quem nao compareceu nao precisa ser filtrado antes.
    Ordene decrescente pela media.

    Resposta esperada (media geral, 2 casas):
    Sul Catarinense 544.67, Norte Catarinense 543.56, Oeste Catarinense
    543.03, Grande Florianopolis 540.89, Vale do Itajai 540.88, Serrana
    538.90.
    """
    # TODO Exercicio 13: join + groupBy("regiao") + agg(F.round(F.avg(...), 2))
    return None


def municipios_destaque(limpo: DataFrame) -> DataFrame:
    """Exercicio 14 -- municipios com 700 ou mais presentes aprovados.

    Agrupe por `municipio`, conte quantas linhas tem `presente == "S"` (dica:
    F.sum(F.when(F.col("presente") == "S", 1).otherwise(0))) e filtre o
    resultado da agregacao (nao antes do groupBy -- isso e HAVING, nao WHERE,
    igual ao exercicio 12 do 03_etl_local.py) para manter so quem tem 700 ou
    mais. Ordene decrescente.

    Resposta esperada: 4 municipios -- Sao Jose 730, Florianopolis 718,
    Joinville 714, Blumenau 710.
    """
    # TODO Exercicio 14: groupBy("municipio") + agg(...) + filter(... >= 700)
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio
# ---------------------------------------------------------------------------
def carregar(
    limpo: DataFrame,
    resumo_regiao: DataFrame,
    destaque: DataFrame,
    rejeitados: DataFrame,
) -> None:
    destino_fato = SAIDA / "enem"
    destino_resumo = SAIDA / "resumo_regiao"
    destino_destaque = SAIDA / "municipios_destaque"
    destino_rejeitados = SAIDA / "enem_rejeitados"

    (
        limpo
        .repartition("municipio")
        .write.mode("overwrite")          # overwrite e o que torna o ETL repetivel
        .partitionBy("municipio")
        .parquet(str(destino_fato))
    )
    resumo_regiao.coalesce(1).write.mode("overwrite").parquet(str(destino_resumo))
    destaque.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_destaque)
    )
    rejeitados.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitados)
    )

    for pasta in (destino_fato, destino_resumo, destino_destaque, destino_rejeitados):
        dados = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<20} {len(dados):>3} arquivo(s) de dados")


def main() -> None:
    spark = criar_sessao("aula03-05-etl-enem-sc")
    try:
        titulo("0. Fonte")
        gerar_csv_enem()
        gerar_csv_municipios()

        titulo("1. Extract")
        bruto, municipios = extrair(spark)

        titulo("2. Transform -- normalizar (exercicio 10)")
        normalizado = normalizar(bruto)
        total_bruto = bruto.count()
        total_normalizado = normalizado.count()
        print(f"lidas .............: {total_bruto}")
        print(f"apos dropDuplicates: {total_normalizado}"
              f"  (-{total_bruto - total_normalizado} duplicatas)")

        titulo("3. Transform -- validar (exercicio 11)")
        aprovados, rejeitados = validar(normalizado)
        total_aprovado = aprovados.count()
        total_rejeitado = rejeitados.count()
        print(f"aprovadas .........: {total_aprovado}")
        print(f"rejeitadas ........: {total_rejeitado}")
        # Conciliacao: toda linha normalizada tem de sair de um dos dois lados.
        assert total_aprovado + total_rejeitado == total_normalizado, (
            f"linhas perdidas: {total_normalizado - total_aprovado - total_rejeitado}"
        )
        print("conciliacao .......: OK")
        rejeitados.groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)

        titulo("4. Transform -- enriquecer (exercicio 12)")
        limpo = enriquecer(aprovados)
        limpo.cache()
        limpo.groupBy("faixa_desempenho").count().orderBy(F.desc("count")).show()

        titulo("5. Transform -- resumo por regiao (exercicio 13)")
        resumo_regiao = resumir_por_regiao(limpo, municipios)
        resumo_regiao.show()

        titulo("6. Transform -- municipios em destaque (exercicio 14)")
        destaque = municipios_destaque(limpo)
        destaque.show()

        titulo("7. Load")
        carregar(limpo, resumo_regiao, destaque, rejeitados)

        titulo("Resumo da execucao")
        print(f"entrada ......: {ARQUIVO_ENEM}")
        print(f"lidas ........: {total_bruto}")
        print(f"aprovadas ....: {total_aprovado}")
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
