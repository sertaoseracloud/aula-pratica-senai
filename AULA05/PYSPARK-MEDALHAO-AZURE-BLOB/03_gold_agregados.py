"""Job 3/3 -- camada Gold: classificar, juntar com regiao e agregar por negocio.

    python 03_gold_agregados.py

Le a Silver DO BLOB, classifica cada leitura, junta com a tabela de regiao e
agrega por negocio. Grava a Gold de volta no Blob.

Este e um job INDEPENDENTE: precisa que `02_silver_limpeza.py` ja tenha
gravado `silver/pluviometria/` no Blob. Tambem gera e semeia, na primeira
execucao, a tabela de apoio `municipio -> regiao` -- ela nao passa pela
Bronze nem pela Silver de proposito: e referencia praticamente estatica, nao
um fato que chega todo dia.

    floci-az silver/pluviometria/       entrada (gravada pelo job 2)
    dados/municipios_sc.csv             tabela de apoio (gerada e enviada uma vez)
    floci-az referencia/municipios_sc.csv  a mesma tabela, no Blob
    camadas/gold/fato_pluviometria/     copia local, antes de subir
    floci-az gold/fato_pluviometria/    saida: fato classificado, no Blob
    floci-az gold/resumo_regiao/        saida: total trimestral e dias de chuva forte, no Blob
"""

import csv
import shutil

from comum import (
    CAMADAS,
    DADOS,
    baixar_arquivo,
    baixar_pasta,
    criar_sessao,
    obter_container_client,
    prefixo_existe,
    subir_arquivo,
    subir_pasta,
    titulo,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ARQUIVO_MUNICIPIOS = DADOS / "municipios_sc.csv"

# municipio -> regiao
MUNICIPIOS_REGIOES = {
    "Florianopolis": "Grande Florianopolis",
    "Sao Jose": "Grande Florianopolis",
    "Joinville": "Norte Catarinense",
    "Jaragua do Sul": "Norte Catarinense",
    "Blumenau": "Vale do Itajai",
    "Itajai": "Vale do Itajai",
    "Balneario Camboriu": "Vale do Itajai",
    "Chapeco": "Oeste Catarinense",
    "Criciuma": "Sul Catarinense",
    "Lages": "Serrana",
}


# ---------------------------------------------------------------------------
# Fonte -- dado, sem exercicio: a tabela de apoio (municipio -> regiao)
# ---------------------------------------------------------------------------
def gerar_e_semear_municipios(container) -> None:
    if not ARQUIVO_MUNICIPIOS.exists():
        DADOS.mkdir(parents=True, exist_ok=True)
        with ARQUIVO_MUNICIPIOS.open("w", newline="", encoding="utf-8") as fh:
            escritor = csv.writer(fh)
            escritor.writerow(["municipio", "regiao"])
            for municipio, regiao in MUNICIPIOS_REGIOES.items():
                escritor.writerow([municipio.upper(), regiao])
        print(f"entrada gerada: {ARQUIVO_MUNICIPIOS}")
    else:
        print(f"entrada ja existe: {ARQUIVO_MUNICIPIOS} (nao regerada)")

    remoto = "referencia/municipios_sc.csv"
    if container.get_blob_client(remoto).exists():
        print(f"ja esta no Blob: {remoto} (nao reenviado)")
    else:
        subir_arquivo(container, ARQUIVO_MUNICIPIOS, remoto)
        print(f"enviado ao Blob: {ARQUIVO_MUNICIPIOS.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar a Silver e a tabela de apoio
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, container) -> tuple[DataFrame, DataFrame]:
    if not prefixo_existe(container, "silver/pluviometria"):
        raise SystemExit(
            "Silver nao encontrada no Blob (prefixo silver/pluviometria). "
            "Rode antes: python 01_bronze_ingestao.py && python 02_silver_limpeza.py"
        )
    destino = CAMADAS / "silver" / "pluviometria"
    baixados = baixar_pasta(container, "silver/pluviometria", destino)
    print(f"arquivos baixados da Silver: {baixados}")

    baixar_arquivo(container, "referencia/municipios_sc.csv", DADOS / "_baixado_municipios.csv")

    silver = spark.read.parquet(str(destino))
    municipios = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(DADOS / "_baixado_municipios.csv"))
    )
    print("linhas lidas da Silver ..:", silver.count())
    print("linhas lidas (municipios):", municipios.count())
    silver.show(5, truncate=False)
    return silver, municipios


# ---------------------------------------------------------------------------
# T de Transform -- dois blocos, dois exercicios
# ---------------------------------------------------------------------------
def enriquecer(silver: DataFrame, municipios: DataFrame) -> DataFrame:
    """Exercicio 4 -- classificar a chuva do dia e trazer a regiao.

    Acrescente `faixa_chuva` e junte com `municipios`:
    - `faixa_chuva`: "seco" quando chuva_mm < 1, "moderado" quando <= 20,
      "forte" acima disso.
    - `join(municipios, on="municipio", how="left")` para trazer `regiao`.

    Selecione ao final: leitura_id, data, municipio, regiao, chuva_mm,
    umidade_pct, faixa_chuva.

    Resposta esperada: 885 linhas (mesma contagem da Silver aprovada).
    Faixas: seco 576, moderado 165, forte 144.
    """
    # TODO Exercicio 4: withColumn("faixa_chuva", ...), join com municipios, select final.
    return None


def resumir_por_regiao(fato: DataFrame) -> DataFrame:
    """Exercicio 5 -- total trimestral de chuva e dias de chuva forte, por regiao.

    Agrupe `fato` por `regiao` e calcule:
    - `leituras`: F.count("*").
    - `chuva_total_mm`: F.round(F.sum("chuva_mm"), 1).
    - `dias_chuva_forte`: F.sum(F.when(F.col("faixa_chuva") == "forte", 1).otherwise(0)).

    Ordene decrescente por `chuva_total_mm`.

    Resposta esperada (mm, decrescente):

        Vale do Itajai            2160.4  (51 dias de chuva forte)
        Norte Catarinense         1414.0  (34 dias de chuva forte)
        Grande Florianopolis      1278.9  (30 dias de chuva forte)
        Serrana                    528.4  ( 9 dias de chuva forte)
        Oeste Catarinense          493.2  (10 dias de chuva forte)
        Sul Catarinense            379.4  (10 dias de chuva forte)
    """
    # TODO Exercicio 5: groupBy("regiao") + agg(...) + orderBy(F.desc(...))
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar local, depois subir a Gold
# ---------------------------------------------------------------------------
def carregar_gold(fato: DataFrame, resumo_regiao: DataFrame, container) -> None:
    destino_fato = CAMADAS / "gold" / "fato_pluviometria"
    destino_resumo = CAMADAS / "gold" / "resumo_regiao"

    (
        fato
        .repartition("municipio")
        .write.mode("overwrite")
        .partitionBy("municipio")
        .parquet(str(destino_fato))
    )
    resumo_regiao.coalesce(1).write.mode("overwrite").parquet(str(destino_resumo))

    for pasta, prefixo in (
        (destino_fato, "gold/fato_pluviometria"),
        (destino_resumo, "gold/resumo_regiao"),
    ):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<20} {len(arquivos):>3} arquivo(s) local(is)")
        enviados = subir_pasta(container, pasta, prefixo)
        print(f"{'':<20} {enviados:>3} arquivo(s) enviado(s) ao Blob ({prefixo}/)")


def main() -> None:
    spark = criar_sessao("aula05-blob-03-gold")
    try:
        container = obter_container_client()

        titulo("0. Fonte -- tabela de apoio (municipio -> regiao)")
        gerar_e_semear_municipios(container)

        titulo("1. Extract -- baixar a Silver e a tabela de apoio")
        silver, municipios = extrair(spark, container)

        titulo("2. Transform -- classificar e juntar com a regiao (exercicio 4)")
        fato = enriquecer(silver, municipios)
        fato.cache()
        total_fato = fato.count()
        print(f"linhas no fato ....: {total_fato}")
        fato.groupBy("faixa_chuva").count().orderBy(F.desc("count")).show()

        titulo("3. Transform -- resumo por regiao (exercicio 5)")
        resumo_regiao = resumir_por_regiao(fato)
        resumo_regiao.show()

        titulo("4. Load -- gravar e subir a Gold")
        carregar_gold(fato, resumo_regiao, container)

        titulo("Resumo do job")
        print(f"linhas no fato ....: {total_fato}")
        print("pipeline completo -- Bronze -> Silver -> Gold, tudo no Blob")
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
