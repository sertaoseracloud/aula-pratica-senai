"""Job 3/3 -- camada Gold: classificar, juntar com regiao e agregar por negocio.

    python 03_gold_agregados.py

Le a Silver DO S3, classifica cada leitura pela taxa de reciclagem, junta com
a tabela de regiao e agrega por negocio. Grava a Gold de volta no S3.

Este e um job INDEPENDENTE: precisa que `02_silver_limpeza.py` ja tenha
gravado `silver/residuos/` no S3. Tambem gera e semeia, na primeira
execucao, a tabela de apoio `municipio -> regiao`.

    S3 silver/residuos/                entrada (gravada pelo job 2)
    dados/municipios_sc.csv            tabela de apoio (gerada e enviada uma vez)
    S3 referencia/municipios_sc.csv    a mesma tabela, no bucket
    camadas/gold/fato_residuos/        copia local, antes de subir
    S3 gold/fato_residuos/             saida: fato classificado, no bucket
    S3 gold/resumo_regiao/             saida: total trimestral e taxa media, no bucket
"""

import csv
import shutil

from comum import (
    BUCKET,
    CAMADAS,
    DADOS,
    baixar_arquivo,
    baixar_pasta,
    criar_sessao,
    obter_bucket,
    objeto_existe,
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
def gerar_e_semear_municipios(s3) -> None:
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
    if objeto_existe(s3, remoto):
        print(f"ja esta no S3: {remoto} (nao reenviado)")
    else:
        subir_arquivo(s3, ARQUIVO_MUNICIPIOS, remoto)
        print(f"enviado ao S3: {ARQUIVO_MUNICIPIOS.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar a Silver e a tabela de apoio
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, s3) -> tuple[DataFrame, DataFrame]:
    if not prefixo_existe(s3, "silver/residuos"):
        raise SystemExit(
            "Silver nao encontrada no S3 (prefixo silver/residuos). "
            "Rode antes: python 01_bronze_ingestao.py && python 02_silver_limpeza.py"
        )
    destino = CAMADAS / "silver" / "residuos"
    baixados = baixar_pasta(s3, "silver/residuos", destino)
    print(f"arquivos baixados da Silver: {baixados}")

    baixar_arquivo(s3, "referencia/municipios_sc.csv", DADOS / "_baixado_municipios.csv")

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
    """Exercicio 4 -- calcular a taxa de reciclagem, classificar e trazer a regiao.

    Acrescente `taxa_reciclagem_pct` e `faixa_reciclagem`, depois junte com
    `municipios`:

    - `taxa_reciclagem_pct`: F.round(toneladas_reciclavel / toneladas_total * 100, 1).
    - `faixa_reciclagem`: "baixa" quando taxa_reciclagem_pct < 15, "media"
      quando <= 30, "alta" acima disso.
    - `join(municipios, on="municipio", how="left")` para trazer `regiao`.

    Selecione ao final: leitura_id, data, municipio, regiao,
    toneladas_total, toneladas_reciclavel, taxa_reciclagem_pct, faixa_reciclagem.

    Resposta esperada: 856 linhas (mesma contagem da Silver aprovada).
    Faixas: baixa 258, media 577, alta 21.
    """
    # TODO Exercicio 4: withColumn("taxa_reciclagem_pct", ...), withColumn("faixa_reciclagem", ...),
    # join com municipios, select final.
    return None


def resumir_por_regiao(fato: DataFrame) -> DataFrame:
    """Exercicio 5 -- total trimestral coletado e taxa media de reciclagem, por regiao.

    Agrupe `fato` por `regiao` e calcule:
    - `leituras`: F.count("*").
    - `toneladas_total_trimestre`: F.round(F.sum("toneladas_total"), 1).
    - `taxa_reciclagem_media_pct`: F.round(F.avg("taxa_reciclagem_pct"), 2).

    Ordene decrescente por `toneladas_total_trimestre`.

    Resposta esperada (t = toneladas, decrescente por total coletado):

        Grande Florianopolis     53153.0 t   17.80%
        Vale do Itajai           51770.7 t   18.20%
        Norte Catarinense        40463.7 t   18.86%
        Oeste Catarinense        16122.6 t   17.13%
        Sul Catarinense          15432.6 t   17.85%
        Serrana                  11001.5 t   18.76%
    """
    # TODO Exercicio 5: groupBy("regiao") + agg(...) + orderBy(F.desc(...))
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar local, depois subir a Gold
# ---------------------------------------------------------------------------
def carregar_gold(fato: DataFrame, resumo_regiao: DataFrame, s3) -> None:
    destino_fato = CAMADAS / "gold" / "fato_residuos"
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
        (destino_fato, "gold/fato_residuos"),
        (destino_resumo, "gold/resumo_regiao"),
    ):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<18} {len(arquivos):>3} arquivo(s) local(is)")
        enviados = subir_pasta(s3, pasta, prefixo)
        print(f"{'':<18} {enviados:>3} arquivo(s) enviado(s) ao S3 ({prefixo}/)")


def main() -> None:
    spark = criar_sessao("aula05-s3-03-gold")
    try:
        s3 = obter_bucket()

        titulo("0. Fonte -- tabela de apoio (municipio -> regiao)")
        gerar_e_semear_municipios(s3)

        titulo("1. Extract -- baixar a Silver e a tabela de apoio")
        silver, municipios = extrair(spark, s3)

        titulo("2. Transform -- taxa de reciclagem e regiao (exercicio 4)")
        fato = enriquecer(silver, municipios)
        fato.cache()
        total_fato = fato.count()
        print(f"linhas no fato ....: {total_fato}")
        fato.groupBy("faixa_reciclagem").count().orderBy(F.desc("count")).show()

        titulo("3. Transform -- resumo por regiao (exercicio 5)")
        resumo_regiao = resumir_por_regiao(fato)
        resumo_regiao.show()

        titulo("4. Load -- gravar e subir a Gold")
        carregar_gold(fato, resumo_regiao, s3)

        titulo("Resumo do job")
        print(f"bucket ............: {BUCKET}")
        print(f"linhas no fato ....: {total_fato}")
        print("pipeline completo -- Bronze -> Silver -> Gold, tudo no S3")
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
