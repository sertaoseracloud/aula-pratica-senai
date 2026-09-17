"""Job 3/3 -- camada Gold: classificar, juntar com regiao e agregar por negocio.

    python 03_gold_agregados.py

A Gold le o que a Silver gravou -- dado ja limpo e validado -- e aplica a
ultima camada de regra: a que so faz sentido para quem vai CONSUMIR o dado
(um dashboard, um relatorio), nao para quem esta garantindo qualidade. Aqui
entra a classificacao (bom/moderado/ruim) e a agregacao por regiao.

Este e um job INDEPENDENTE: precisa que `02_silver_limpeza.py` ja tenha
rodado (ele le `camadas/silver/`). Tambem gera, na primeira execucao, a
tabela de apoio `municipio -> regiao` -- ela nao passa pela Bronze nem pela
Silver de proposito: e uma tabela de referencia praticamente estatica, nao
um fato que chega todo dia.

    camadas/silver/qualidade_ar/          entrada (gravada pelo job 2)
    dados/municipios_sc.csv               tabela de apoio (gerada na primeira execucao)
    camadas/gold/fato_qualidade_ar/       saida: fato enriquecido, particionado por municipio
    camadas/gold/resumo_regiao/           saida: media trimestral e dias ruins por regiao
"""

import csv
import shutil

from comum import CAMADAS, DADOS, GOLD, SILVER, criar_sessao, titulo

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
def gerar_csv_municipios() -> None:
    if ARQUIVO_MUNICIPIOS.exists():
        print(f"entrada ja existe: {ARQUIVO_MUNICIPIOS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    with ARQUIVO_MUNICIPIOS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(["municipio", "regiao"])
        for municipio, regiao in MUNICIPIOS_REGIOES.items():
            escritor.writerow([municipio.upper(), regiao])

    print(f"entrada gerada: {ARQUIVO_MUNICIPIOS}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: ler o que a Silver gravou
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    origem = SILVER / "qualidade_ar"
    if not origem.exists():
        raise SystemExit(
            f"Silver nao encontrada em {origem}. Rode antes: "
            "python 01_bronze_ingestao.py && python 02_silver_limpeza.py"
        )
    silver = spark.read.parquet(str(origem))
    municipios = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(ARQUIVO_MUNICIPIOS))
    )
    print("linhas lidas da Silver ..:", silver.count())
    print("linhas lidas (municipios):", municipios.count())
    silver.show(5, truncate=False)
    return silver, municipios


# ---------------------------------------------------------------------------
# T de Transform -- dois blocos, dois exercicios
# ---------------------------------------------------------------------------
def enriquecer(silver: DataFrame, municipios: DataFrame) -> DataFrame:
    """Exercicio 4 -- classificar a qualidade do ar e trazer a regiao.

    Acrescente `faixa_qualidade` e junte com `municipios`:
    - `faixa_qualidade`: "bom" quando pm25 <= 25, "moderado" quando <= 50,
      "ruim" acima disso -- uma cadeia de F.when.
    - `join(municipios, on="municipio", how="left")` para trazer `regiao`.

    Selecione ao final: leitura_id, data, municipio, regiao, pm25, pm10,
    co_ppm, faixa_qualidade.

    Resposta esperada: 889 linhas (mesma contagem da Silver aprovada).
    Faixas: bom 838, moderado 51, ruim 0.
    """
    # TODO Exercicio 4: withColumn("faixa_qualidade", ...), join com municipios, select final.
    return None


def resumir_por_regiao(fato: DataFrame) -> DataFrame:
    """Exercicio 5 -- media trimestral de pm2.5 e dias moderados ou piores, por regiao.

    Agrupe `fato` por `regiao` e calcule:
    - `leituras`: F.count("*").
    - `pm25_medio`: F.round(F.avg("pm25"), 2).
    - `dias_moderados_ou_piores`: F.sum(F.when(F.col("faixa_qualidade") != "bom", 1).otherwise(0)).

    Ordene decrescente por `pm25_medio`.

    Resposta esperada (desc por pm25_medio):

        Sul Catarinense           22.59  (90 leituras, 21 moderados/piores)
        Norte Catarinense         19.85  (175 leituras, 21 moderados/piores)
        Serrana                   18.03  (89 leituras, 1 moderado/pior)
        Vale do Itajai            17.55  (271 leituras, 8 moderados/piores)
        Oeste Catarinense         15.42  (87 leituras, 0 moderados/piores)
        Grande Florianopolis      14.08  (177 leituras, 0 moderados/piores)
    """
    # TODO Exercicio 5: groupBy("regiao") + agg(...) + orderBy(F.desc(...))
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar a Gold
# ---------------------------------------------------------------------------
def carregar_gold(fato: DataFrame, resumo_regiao: DataFrame) -> None:
    destino_fato = GOLD / "fato_qualidade_ar"
    destino_resumo = GOLD / "resumo_regiao"

    (
        fato
        .repartition("municipio")
        .write.mode("overwrite")
        .partitionBy("municipio")
        .parquet(str(destino_fato))
    )
    resumo_regiao.coalesce(1).write.mode("overwrite").parquet(str(destino_resumo))

    for pasta in (destino_fato, destino_resumo):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<20} {len(arquivos):>3} arquivo(s) de dados")


def main() -> None:
    spark = criar_sessao("aula05-03-gold-agregados")
    try:
        titulo("0. Fonte -- tabela de apoio (municipio -> regiao)")
        gerar_csv_municipios()

        titulo("1. Extract -- ler a Silver e a tabela de apoio")
        silver, municipios = extrair(spark)

        titulo("2. Transform -- classificar e juntar com a regiao (exercicio 4)")
        fato = enriquecer(silver, municipios)
        fato.cache()
        total_fato = fato.count()
        print(f"linhas no fato ....: {total_fato}")
        fato.groupBy("faixa_qualidade").count().orderBy(F.desc("count")).show()

        titulo("3. Transform -- resumo por regiao (exercicio 5)")
        resumo_regiao = resumir_por_regiao(fato)
        resumo_regiao.show()

        titulo("4. Load -- gravar a Gold")
        carregar_gold(fato, resumo_regiao)

        titulo("Resumo do job")
        print(f"lidas da Silver ...: {total_fato}")
        print(f"saida .............: {GOLD}")
        print("pipeline completo -- Bronze -> Silver -> Gold")
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
