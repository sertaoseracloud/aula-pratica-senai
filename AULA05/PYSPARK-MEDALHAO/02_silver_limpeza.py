"""Job 2/3 -- camada Silver: normalizar, validar e separar o que presta.

    python 02_silver_limpeza.py

A Silver le o que a Bronze gravou -- nunca o CSV original -- e faz o
trabalho que a Bronze deixou de propósito para depois: padronizar texto,
remover duplicata e decidir o que e uma leitura valida, com o motivo de
cada rejeicao. O que sai daqui e "dado em que se pode confiar", mas ainda
sem nenhuma agregacao de negocio -- isso fica para a Gold.

Este e um job INDEPENDENTE: precisa que `01_bronze_ingestao.py` ja tenha
rodado (ele le `camadas/bronze/`). Se a Bronze nao existir, este job para
com uma mensagem clara, nao com um traceback de arquivo nao encontrado sem
contexto.

    camadas/bronze/qualidade_ar/          entrada (gravada pelo job 1)
    camadas/silver/qualidade_ar/          saida: leituras validas, sem duplicata
    camadas/silver/qualidade_ar_rejeitada/ saida: leituras invalidas, com motivo
"""

import shutil

from comum import BRONZE, CAMADAS, SILVER, criar_sessao, titulo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

NOMES_MUNICIPIOS = [
    "Florianopolis", "Sao Jose", "Joinville", "Jaragua do Sul", "Blumenau",
    "Itajai", "Balneario Camboriu", "Chapeco", "Criciuma", "Lages",
]


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: ler o que a Bronze gravou
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession) -> DataFrame:
    origem = BRONZE / "qualidade_ar"
    if not origem.exists():
        raise SystemExit(
            f"Bronze nao encontrada em {origem}. Rode primeiro: python 01_bronze_ingestao.py"
        )
    bronze = spark.read.parquet(str(origem))
    print("linhas lidas da Bronze:", bronze.count())
    bronze.show(5, truncate=False)
    return bronze


# ---------------------------------------------------------------------------
# T de Transform -- dois blocos, dois exercicios
# ---------------------------------------------------------------------------
def normalizar(bronze: DataFrame) -> DataFrame:
    """Exercicio 2 -- padronizar municipio, converter a data e tirar duplicatas.

    Tres coisas, nesta ordem:
    1. `municipio` para maiusculo e sem espaco nas pontas.
    2. `data` de string para date, com F.to_date("data", "yyyy-MM-dd").
    3. dropDuplicates em ["leitura_id", "data", "municipio"].

    As colunas de proveniencia (`arquivo_origem`, `ingerido_em`) continuam
    na tabela -- a Silver nao apaga historico, so limpa o que e conteudo.

    Resposta esperada: 920 linhas (3 duplicatas removidas de 923 lidas).
    """
    # TODO Exercicio 2: normalizar municipio, converter a data e remover duplicatas.
    return None


def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Exercicio 3 -- separar leituras validas de invalidas, com o motivo.

    Uma leitura e valida quando, ao mesmo tempo:
    - `municipio` esta em NOMES_MUNICIPIOS (compare em maiusculo);
    - `pm25` <= `pm10` (fisicamente, particulado fino e um subconjunto do grosso);
    - `pm25` e `pm10` estao dentro de [0, 500];
    - `co_ppm` esta dentro de [0, 50].

    Junte as quatro condicoes com `&` dentro de um F.coalesce(..., F.lit(False))
    -- o mesmo padrao usado nos exercicios de ETL da AULA04. Sem o coalesce,
    uma condicao que vira NULL faz a linha inteira sumir dos dois lados sem
    que a conciliacao note.

    Monte a coluna `motivo` nos rejeitados com uma cadeia de F.when, nesta
    ordem de prioridade: municipio invalido, depois pm25 > pm10, depois
    particulado fora da faixa fisica, e o que sobrar (`.otherwise(...)`) e
    CO fora da faixa.

    Resposta esperada: 889 aprovadas, 31 rejeitadas.

        co fora da faixa (0 a 50)                    11
        pm25 maior que pm10                           9
        municipio invalida ou ausente                 7
        particulado fora da faixa fisica (0 a 500)    4
    """
    nomes_validos = [m.upper() for m in NOMES_MUNICIPIOS]

    # TODO Exercicio 3: regra_valida = F.coalesce(..., F.lit(False))
    regra_valida = None

    rejeitadas = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.lit(None).cast("string"),  # TODO Exercicio 3: cadeia de F.when(...).otherwise(...)
    )
    aprovadas = normalizado.filter(regra_valida)
    return aprovadas, rejeitadas


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar a Silver
# ---------------------------------------------------------------------------
def carregar_silver(aprovadas: DataFrame, rejeitadas: DataFrame) -> None:
    destino_ok = SILVER / "qualidade_ar"
    destino_rejeitadas = SILVER / "qualidade_ar_rejeitada"

    aprovadas.write.mode("overwrite").parquet(str(destino_ok))
    rejeitadas.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitadas)
    )

    for pasta in (destino_ok, destino_rejeitadas):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<24} {len(arquivos):>3} arquivo(s) de dados")


def main() -> None:
    spark = criar_sessao("aula05-02-silver-limpeza")
    try:
        titulo("1. Extract -- ler a Bronze")
        bronze = extrair(spark)

        titulo("2. Transform -- normalizar (exercicio 2)")
        normalizado = normalizar(bronze)
        total_bronze = bronze.count()
        total_normalizado = normalizado.count()
        print(f"lidas .............: {total_bronze}")
        print(f"apos dropDuplicates: {total_normalizado}"
              f"  (-{total_bronze - total_normalizado} duplicatas)")

        titulo("3. Transform -- validar (exercicio 3)")
        aprovadas, rejeitadas = validar(normalizado)
        total_aprovado = aprovadas.count()
        total_rejeitado = rejeitadas.count()
        print(f"aprovadas .........: {total_aprovado}")
        print(f"rejeitadas ........: {total_rejeitado}")
        # Conciliacao: toda linha normalizada tem de sair de um dos dois lados.
        assert total_aprovado + total_rejeitado == total_normalizado, (
            f"linhas perdidas: {total_normalizado - total_aprovado - total_rejeitado}"
        )
        print("conciliacao .......: OK")
        rejeitadas.groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)

        titulo("4. Load -- gravar a Silver")
        carregar_silver(aprovadas, rejeitadas)

        titulo("Resumo do job")
        print(f"lidas da Bronze ...: {total_bronze}")
        print(f"aprovadas .........: {total_aprovado}")
        print(f"rejeitadas ........: {total_rejeitado}")
        print(f"saida .............: {SILVER}")
        print("proximo job .......: 03_gold_agregados.py")
    finally:
        spark.stop()


def limpar_camadas() -> None:
    """Apaga as tres camadas, preservando a entrada -- util para reprocessar do zero."""
    if CAMADAS.exists():
        shutil.rmtree(CAMADAS)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_camadas()
        print(f"camadas removidas: {CAMADAS}")
    main()
