"""Segmenta clientes e projeta features sem utilizar rotulos-alvo."""

from __future__ import annotations

from comum import GOLD, SILVER, criar_sessao, titulo
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

COLUNAS_FEATURE = [
    "gasto_mensal", "compras_mes", "dias_desde_ultima_compra",
    "visitas_mes", "devolucoes_pct", "gasto_por_compra",
]


def segmentar_clientes(dados):
    """Exercicio 1 -- descobrir segmentos sem usar uma coluna-alvo.

     Nesta ordem:
     1. Use COLUNAS_FEATURE; este conjunto nao tem rotulo verdadeiro.
     2. Encadeie VectorAssembler, StandardScaler (withMean=True,
         withStd=True) e KMeans (k=3, seed=42) em um Pipeline.
     3. Ajuste em todos os dados, transforme-os e obtenha trainingCost do
         modelo K-Means.
     4. Retorne (segmentos, modelo, wssse).

     Resposta esperada: 900 clientes distribuidos nos segmentos 0, 1 e 2;
         os IDs sao arbitrarios e WSSSE nao e acuracia.
     """
    # TODO: assembler, scaler e KMeans; retorne segmentos, modelo e WSSSE.
    return None


def projetar_clientes(dados):
    """Exercicio 2 -- projetar features sem classe em duas dimensoes.

     Nesta ordem:
     1. Use COLUNAS_FEATURE, sem incluir alvo ou segmento.
     2. Encadeie VectorAssembler, StandardScaler (withMean=True,
         withStd=True) e PCA (k=2, outputCol="componentes") em um Pipeline.
     3. Ajuste em todos os dados Silver e transforme as linhas.
     4. Retorne (projecao, modelo).

     Resposta esperada: 900 linhas com cliente_id e um vetor de duas
         componentes, convertido pelo job em pc1 e pc2.
     """
    # TODO: assembler, scaler e PCA(k=2); retorne projecao e modelo.
    return None


def main() -> None:
    spark = criar_sessao("aula06-nao-supervisionado-03-gold")
    try:
        origem = SILVER / "comportamento_clientes"
        if not origem.exists():
            raise SystemExit(
                f"Silver ausente em {origem}. Rode Bronze e Silver primeiro."
            )
        dados = spark.read.parquet(str(origem))

        titulo("K-Means: descoberta de segmentos de clientes")
        segmentos, modelo_kmeans, wssse = segmentar_clientes(dados)
        segmentos.select(
            "cliente_id", "segmento", *COLUNAS_FEATURE
        ).write.mode("overwrite").parquet(str(GOLD / "segmentos_clientes"))
        modelo_kmeans.write().overwrite().save(
            str(GOLD / "modelos" / "clientes_kmeans")
        )
        segmentos.groupBy("segmento").agg(
            F.count("*").alias("clientes"),
            F.round(F.avg("gasto_mensal"), 2).alias("gasto_medio"),
            F.round(F.avg("compras_mes"), 2).alias("compras_mensais_medias"),
            F.round(F.avg("dias_desde_ultima_compra"), 2).alias(
                "recencia_media_dias"
            ),
        ).orderBy("segmento").show(truncate=False)

        titulo("PCA: projecao dos clientes em duas dimensoes")
        projecao, modelo_pca = projetar_clientes(dados)
        coordenadas = projecao.withColumn(
            "componentes_array", vector_to_array(F.col("componentes"))
        ).select(
            "cliente_id",
            F.col("componentes_array")[0].alias("pc1"),
            F.col("componentes_array")[1].alias("pc2"),
        )
        coordenadas.write.mode("overwrite").parquet(
            str(GOLD / "projecao_clientes_pca")
        )
        modelo_pca.write().overwrite().save(
            str(GOLD / "modelos" / "clientes_pca")
        )
        spark.createDataFrame(
            [("wssse_kmeans", float(wssse))], ["metrica", "valor"]
        ).coalesce(1).write.mode("overwrite").parquet(
            str(GOLD / "metricas")
        )
        print(f"WSSSE={wssse:.2f}; saídas sem rótulos gravadas em {GOLD}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
