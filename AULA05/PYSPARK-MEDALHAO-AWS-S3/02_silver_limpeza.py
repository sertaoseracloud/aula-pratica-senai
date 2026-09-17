"""Job 2/3 -- camada Silver: normalizar, validar e separar o que presta.

    python 02_silver_limpeza.py

Le a Bronze DO S3 -- nunca o CSV original -- normaliza, remove duplicata e
separa leituras validas de invalidas, com o motivo de cada rejeicao. Grava a
Silver de volta no S3, mais a tabela de rejeitadas para auditoria.

Este e um job INDEPENDENTE: precisa que `01_bronze_ingestao.py` ja tenha
gravado `bronze/residuos/` no S3. Se esse prefixo nao existir, o job para
com uma mensagem clara.

    S3 bronze/residuos/           entrada (gravada pelo job 1)
    camadas/bronze/residuos/      copia local da Bronze, baixada para ler
    camadas/silver/residuos/      copia local da Silver, antes de subir
    S3 silver/residuos/           saida: leituras validas, no bucket
    S3 silver/residuos_rejeitada/ saida: leituras invalidas, com motivo
"""

import shutil

from comum import (
    BUCKET,
    CAMADAS,
    baixar_pasta,
    criar_sessao,
    obter_bucket,
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
# E de Extract -- dado, sem exercicio: baixar a Bronze do S3
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, s3) -> DataFrame:
    if not prefixo_existe(s3, "bronze/residuos"):
        raise SystemExit(
            "Bronze nao encontrada no S3 (prefixo bronze/residuos). "
            "Rode primeiro: python 01_bronze_ingestao.py"
        )
    destino = CAMADAS / "bronze" / "residuos"
    baixados = baixar_pasta(s3, "bronze/residuos", destino)
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

    Resposta esperada: 920 linhas (1 duplicata removida de 921 lidas).
    """
    # TODO Exercicio 2: normalizar municipio, converter a data e remover duplicatas.
    return None


def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Exercicio 3 -- separar leituras validas de invalidas, com o motivo.

    Uma leitura e valida quando, ao mesmo tempo:
    - `municipio` esta em NOMES_MUNICIPIOS (compare em maiusculo);
    - `toneladas_reciclavel` <= `toneladas_total` (fisicamente, o reciclavel
      e uma parte do total, nunca mais que ele);
    - `toneladas_total` esta dentro de [0, 500];
    - `toneladas_reciclavel` esta dentro de [0, 500].

    Junte as quatro condicoes com `&` dentro de um F.coalesce(..., F.lit(False)).

    Monte a coluna `motivo` nos rejeitados com uma cadeia de F.when, nesta
    ordem de prioridade: municipio invalido, depois reciclavel > total,
    depois total fora da faixa fisica, e o que sobrar (`.otherwise(...)`) e
    reciclavel fora da faixa fisica.

    Resposta esperada: 856 aprovadas, 64 rejeitadas.

        total fora da faixa fisica (0 a 500)         47
        reciclavel maior que o total                  12
        municipio invalida ou ausente                  5
        reciclavel fora da faixa fisica (0 a 500)      0
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
def carregar_silver(aprovadas: DataFrame, rejeitadas: DataFrame, s3) -> None:
    destino_ok = CAMADAS / "silver" / "residuos"
    destino_rejeitadas = CAMADAS / "silver" / "residuos_rejeitada"

    aprovadas.write.mode("overwrite").parquet(str(destino_ok))
    rejeitadas.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitadas)
    )

    for pasta, prefixo in (
        (destino_ok, "silver/residuos"),
        (destino_rejeitadas, "silver/residuos_rejeitada"),
    ):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<22} {len(arquivos):>3} arquivo(s) local(is)")
        enviados = subir_pasta(s3, pasta, prefixo)
        print(f"{'':<22} {enviados:>3} arquivo(s) enviado(s) ao S3 ({prefixo}/)")


def main() -> None:
    spark = criar_sessao("aula05-s3-02-silver")
    try:
        s3 = obter_bucket()

        titulo("1. Extract -- baixar a Bronze do S3")
        bronze = extrair(spark, s3)

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
        carregar_silver(aprovadas, rejeitadas, s3)

        titulo("Resumo do job")
        print(f"bucket ............: {BUCKET}")
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
