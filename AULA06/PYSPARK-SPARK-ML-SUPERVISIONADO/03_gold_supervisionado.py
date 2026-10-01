"""Treina modelos supervisionados de classificacao e regressao."""

from __future__ import annotations

from comum import GOLD, SILVER, criar_sessao, titulo
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


def classificar_churn(dados):
    """Exercicio 1 -- prever cancelamento por classificacao supervisionada.

     Nesta ordem:
     1. Divida dados em treino e teste (80/20), com seed=42.
     2. Use tenure_meses, mensalidade, chamados_suporte e contrato_meses
         como features. `cancelou` e o rotulo, nao uma feature.
     3. Encadeie VectorAssembler e LogisticRegression (maxIter=40,
         regParam=0.05) em um Pipeline; ajuste no treino.
     4. Transforme o teste e avalie areaUnderROC e accuracy.
     5. Retorne previsoes, modelo e metricas no formato
         ("churn", nome_metrica, valor).

     Resposta esperada: previsoes de teste com cliente_id, cancelou,
         prediction e probability; ambas as metricas ficam entre 0 e 1.
     """
    # TODO: assembler, split, LogisticRegression, Pipeline e metricas.
    return None


def prever_demanda(dados):
    """Exercicio 2 -- prever demanda_mwh por regressao supervisionada.

     Nesta ordem:
     1. Divida dados em treino e teste (80/20), com seed=42.
     2. Use temperatura_c, dia_semana, feriado e ocupacao_pct como features;
         `demanda_mwh` e o alvo continuo.
     3. Encadeie VectorAssembler e RandomForestRegressor (numTrees=40,
         maxDepth=6, seed=42) em um Pipeline; ajuste no treino.
     4. Transforme o teste e avalie RMSE e MAE.
     5. Retorne previsoes, modelo e metricas no formato
         ("energia", nome_metrica, valor).

     Resposta esperada: previsoes com registro_id, demanda_mwh e prediction;
         RMSE e MAE positivos, na unidade MWh.
     """
    # TODO: assembler, split, RandomForestRegressor, Pipeline e metricas.
    return None


def main() -> None:
    spark = criar_sessao("aula06-supervisionado-03-gold")
    try:
        churn_path = SILVER / "assinaturas"
        energia_path = SILVER / "demanda_energia"
        if not churn_path.exists() or not energia_path.exists():
            raise SystemExit("Silver ausente. Rode Bronze e Silver primeiro.")

        titulo("Classificacao supervisionada: cancelamento de assinaturas")
        churn = spark.read.parquet(str(churn_path))
        previsoes, modelo, metricas = classificar_churn(churn)
        previsoes.select(
            "cliente_id", "cancelou", "prediction", "probability"
        ).write.mode("overwrite").parquet(str(GOLD / "previsoes_churn"))
        modelo.write().overwrite().save(
            str(GOLD / "modelos" / "churn_logistic_regression")
        )
        metricas_churn = metricas

        titulo("Regressao supervisionada: demanda de energia")
        energia = spark.read.parquet(str(energia_path))
        previsoes, modelo, metricas = prever_demanda(energia)
        previsoes.select(
            "registro_id", "demanda_mwh", "prediction"
        ).write.mode("overwrite").parquet(
            str(GOLD / "previsoes_demanda")
        )
        modelo.write().overwrite().save(
            str(GOLD / "modelos" / "demanda_random_forest")
        )

        todas_metricas = [*metricas_churn, *metricas]
        spark.createDataFrame(
            todas_metricas, ["caso", "metrica", "valor"]
        ).coalesce(1).write.mode("overwrite").parquet(
            str(GOLD / "metricas")
        )
        print(f"Saidas supervisionadas gravadas em {GOLD}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
