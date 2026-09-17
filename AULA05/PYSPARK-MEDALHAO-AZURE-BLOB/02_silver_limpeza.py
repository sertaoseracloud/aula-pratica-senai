"""Job 2/3 -- camada Silver: normalizar, validar e separar o que presta.

    python 02_silver_limpeza.py

Le a Bronze DO BLOB -- nunca o CSV original -- normaliza, remove duplicata e
separa leituras validas de invalidas, com o motivo de cada rejeicao. Grava a
Silver de volta no Blob, mais a tabela de rejeitadas para auditoria.

Este e um job INDEPENDENTE: precisa que `01_bronze_ingestao.py` ja tenha
gravado `bronze/pluviometria/` no Blob. Se esse prefixo nao existir, o job
para com uma mensagem clara.

    floci-az bronze/pluviometria/           entrada (gravada pelo job 1)
    camadas/bronze/pluviometria/            copia local da Bronze, baixada para ler
    camadas/silver/pluviometria/            copia local da Silver, antes de subir
    floci-az silver/pluviometria/           saida: leituras validas, no Blob
    floci-az silver/pluviometria_rejeitada/ saida: leituras invalidas, com motivo
"""

import shutil

from comum import (
    CAMADAS,
    baixar_pasta,
    criar_sessao,
    obter_container_client,
    prefixo_existe,
    subir_pasta,
    titulo,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

NOMES_MUNICIPIOS = [
    "Florianopolis", "Sao Jose", "Joinville", "Jaragua do Sul", "Blumenau",
    "Itajai", "Balneario Camboriu", "Chapeco", "Criciuma", "Lages",
]


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar a Bronze do Blob
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, container) -> DataFrame:
    if not prefixo_existe(container, "bronze/pluviometria"):
        raise SystemExit(
            "Bronze nao encontrada no Blob (prefixo bronze/pluviometria). "
            "Rode primeiro: python 01_bronze_ingestao.py"
        )
    destino = CAMADAS / "bronze" / "pluviometria"
    baixados = baixar_pasta(container, "bronze/pluviometria", destino)
    print(f"arquivos baixados da Bronze: {baixados}")

    bronze = spark.read.parquet(str(destino))
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
    na tabela.

    Resposta esperada: 920 linhas (4 duplicatas removidas de 924 lidas).
    """
    # TODO Exercicio 2: normalizar municipio, converter a data e remover duplicatas.
    return None


def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Exercicio 3 -- separar leituras validas de invalidas, com o motivo.

    Uma leitura e valida quando, ao mesmo tempo:
    - `municipio` esta em NOMES_MUNICIPIOS (compare em maiusculo);
    - `chuva_mm` esta dentro de [0, 200];
    - `umidade_pct` esta dentro de [0, 100].

    Junte as tres condicoes com `&` dentro de um F.coalesce(..., F.lit(False)).

    Monte a coluna `motivo` nos rejeitados com uma cadeia de F.when, nesta
    ordem de prioridade: municipio invalido, depois chuva fora da faixa, e o
    que sobrar (`.otherwise(...)`) e umidade fora da faixa.

    Resposta esperada: 885 aprovadas, 35 rejeitadas.

        chuva fora da faixa (0 a 200)      18
        umidade fora da faixa (0 a 100)    10
        municipio invalida ou ausente       7
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
# L de Load -- dado, sem exercicio: gravar local, depois subir a Silver
# ---------------------------------------------------------------------------
def carregar_silver(aprovadas: DataFrame, rejeitadas: DataFrame, container) -> None:
    destino_ok = CAMADAS / "silver" / "pluviometria"
    destino_rejeitadas = CAMADAS / "silver" / "pluviometria_rejeitada"

    aprovadas.write.mode("overwrite").parquet(str(destino_ok))
    rejeitadas.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitadas)
    )

    for pasta, prefixo in (
        (destino_ok, "silver/pluviometria"),
        (destino_rejeitadas, "silver/pluviometria_rejeitada"),
    ):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<24} {len(arquivos):>3} arquivo(s) local(is)")
        enviados = subir_pasta(container, pasta, prefixo)
        print(f"{'':<24} {enviados:>3} arquivo(s) enviado(s) ao Blob ({prefixo}/)")


def main() -> None:
    spark = criar_sessao("aula05-blob-02-silver")
    try:
        container = obter_container_client()

        titulo("1. Extract -- baixar a Bronze do Blob")
        bronze = extrair(spark, container)

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
        assert total_aprovado + total_rejeitado == total_normalizado, (
            f"linhas perdidas: {total_normalizado - total_aprovado - total_rejeitado}"
        )
        print("conciliacao .......: OK")
        rejeitadas.groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)

        titulo("4. Load -- gravar e subir a Silver")
        carregar_silver(aprovadas, rejeitadas, container)

        titulo("Resumo do job")
        print(f"lidas da Bronze ...: {total_bronze}")
        print(f"aprovadas .........: {total_aprovado}")
        print(f"rejeitadas ........: {total_rejeitado}")
        print("proximo job .......: 03_gold_agregados.py")
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
