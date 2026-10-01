"""Treina modelos Spark ML e publica previsoes, metricas e modelos na Gold."""

from __future__ import annotations

from comum import GOLD, SILVER, criar_sessao, titulo
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F


def classificar_churn(churn):
    """Exercicio 1 -- treinar um classificador supervisionado de churn.

     Nesta ordem:
     1. Divida churn em treino e teste (80/20), com seed=42.
     2. Monte features com tenure_meses, mensalidade, chamados_suporte e
         contrato_meses. `cancelou` e o rotulo, nao uma feature.
     3. Use VectorAssembler e LogisticRegression (maxIter=40, regParam=0.05)
         em um Pipeline e ajuste somente no treino.
     4. Transforme o teste e avalie areaUnderROC e accuracy.
     5. Retorne (previsoes, modelo, metricas), com metricas no formato
         ("churn", nome_metrica, valor).

     Resposta esperada: previsoes de teste com cliente_id, cancelou,
     prediction e probability; AUC e acuracia entre 0 e 1.
    """
    # TODO Exercicio 1: VectorAssembler + LogisticRegression + Pipeline.
    # Separe treino/teste com seed=42 e avalie com os dois evaluators.
    return None


def prever_demanda(energia):
    """Exercicio 2 -- prever a demanda de energia com regressao.

     Nesta ordem:
     1. Divida energia em treino e teste (80/20), com seed=42.
     2. Monte features com temperatura_c, dia_semana, feriado e ocupacao_pct.
         `demanda_mwh` e o alvo continuo.
     3. Use VectorAssembler e RandomForestRegressor (numTrees=40, maxDepth=6,
         seed=42) em um Pipeline e ajuste somente no treino.
     4. Transforme o teste e avalie RMSE e MAE.
     5. Retorne (previsoes, modelo, metricas), com metricas no formato
         ("energia", nome_metrica, valor).

     Resposta esperada: previsoes de teste com registro_id, demanda_mwh e
     prediction; RMSE e MAE positivos, expressos em MWh.
    """
    # TODO Exercicio 2: VectorAssembler + RandomForestRegressor + Pipeline.
    # Separe treino/teste com seed=42 e avalie RMSE e MAE.
    return None


def segmentar_clientes(varejo):
    """Exercicio 3 -- descobrir segmentos sem usar uma coluna-alvo.

     Nesta ordem:
     1. Use gasto_mensal, compras_mes, dias_desde_ultima_compra e
         gasto_por_compra como features; nao ha rotulo neste problema.
     2. Encadeie VectorAssembler, StandardScaler (withMean=True,
         withStd=True) e KMeans (k=3, seed=42) em um Pipeline.
     3. Ajuste em todos os clientes, gere os segmentos e obtenha
         trainingCost do modelo K-Means.
     4. Retorne (segmentos, modelo, metricas), com metricas no formato
         ("varejo", "within_set_sum_squared_error", valor).

     Resposta esperada: 1200 clientes distribuidos nos segmentos 0, 1 e 2.
         Os IDs sao arbitrarios e WSSSE nao e uma medida de acuracia.
    """
    # TODO Exercicio 3: VectorAssembler + StandardScaler + KMeans(k=3).
    # Ajuste em todos os clientes e retorne o custo trainingCost do modelo.
    return None


def salvar_metricas(spark, linhas: list[tuple[str, str, float]]) -> None:
    metricas = spark.createDataFrame(linhas, ["caso_uso", "metrica", "valor"])
    metricas.coalesce(1).write.mode("overwrite").parquet(
        str(GOLD / "metricas")
    )


def main() -> None:
    spark = criar_sessao("aula06-03-gold-spark-ml")
    resultados: list[tuple[str, str, float]] = []
    try:
        titulo("Classificacao: risco de churn em assinaturas")
        churn = spark.read.parquet(str(SILVER / "assinaturas"))
        previsoes_churn, modelo_churn, metricas_churn = classificar_churn(
            churn
        )
        resultados.extend(metricas_churn)
        previsoes_churn.select(
            "cliente_id", "cancelou", "prediction", "probability"
        ).write.mode("overwrite").parquet(str(GOLD / "previsoes_churn"))
        caminho_modelo = GOLD / "modelos" / "churn_logistic_regression"
        modelo_churn.write().overwrite().save(str(caminho_modelo))
        print(f"ROC AUC={metricas_churn[0][2]:.3f} | "
              f"acuracia={metricas_churn[1][2]:.3f}")

        titulo("Regressao: previsao de demanda de energia")
        energia = spark.read.parquet(str(SILVER / "demanda_energia"))
        previsoes_energia, modelo_energia, metricas_energia = prever_demanda(
            energia
        )
        resultados.extend(metricas_energia)
        previsoes_energia.select(
            "registro_id", "demanda_mwh", "prediction"
        ).write.mode("overwrite").parquet(
            str(GOLD / "previsoes_energia")
        )
        caminho_modelo = GOLD / "modelos" / "energia_random_forest"
        modelo_energia.write().overwrite().save(str(caminho_modelo))
        print(f"RMSE={metricas_energia[0][2]:.2f} MWh | "
              f"MAE={metricas_energia[1][2]:.2f} MWh")

        titulo("Clusterizacao: segmentos de clientes de varejo")
        varejo = spark.read.parquet(str(SILVER / "clientes_varejo"))
        segmentos, modelo_segmentos, metricas_varejo = segmentar_clientes(
            varejo
        )
        resultados.extend(metricas_varejo)
        segmentos.select(
            "cliente_id", "gasto_mensal", "compras_mes",
            "dias_desde_ultima_compra", "segmento",
        ).write.mode("overwrite").parquet(str(GOLD / "segmentos_varejo"))
        caminho_modelo = GOLD / "modelos" / "varejo_kmeans"
        modelo_segmentos.write().overwrite().save(str(caminho_modelo))
        segmentos.groupBy("segmento").agg(
            F.count("*").alias("clientes"),
            F.round(F.avg("gasto_mensal"), 2).alias("gasto_medio"),
            F.round(F.avg("compras_mes"), 2).alias("compras_mensais_media"),
        ).orderBy("segmento").show(truncate=False)
        print(
            "KMeans k=3 | custo intra-cluster "
            f"(WSSSE)={metricas_varejo[0][2]:.2f}"
        )

        salvar_metricas(spark, resultados)
        print(f"\nResultados e modelos persistidos em {GOLD}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
