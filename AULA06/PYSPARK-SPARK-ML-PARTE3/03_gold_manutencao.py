"""Seleciona e avalia um classificador de falha para manutencao preditiva."""

from __future__ import annotations

from comum import GOLD, SILVER, criar_sessao, titulo
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


def treinar_modelo_manutencao(dados):
    """Exercicio -- selecionar um classificador para manutencao preditiva.

     Nesta ordem:
     1. Divida dados em treino e teste (80/20), com seed=42.
     2. Monte features com temperatura_c, vibracao_mm_s, pressao_bar,
         horas_desde_manutencao e carga_pct. `falha_24h` e o rotulo.
     3. Use VectorAssembler e GBTClassifier (seed=42) em um Pipeline.
     4. Crie uma grade com maxDepth [3, 5] e maxIter [10, 20]. Ajuste
         CrossValidator com AUC ROC, numFolds=2, seed=42 e parallelism=2
         somente no treino.
     5. Transforme o teste com o CrossValidatorModel e avalie AUC ROC e
         recallByLabel para a classe 1.0.
     6. Retorne (previsoes, cv_model, metricas), com pares
         ("auc_teste", valor) e ("recall_teste", valor).

     Resposta esperada: previsoes de teste com leitura_id, maquina_id,
         falha_24h, prediction e probability; o melhor modelo e o selecionado
         pela AUC ROC media da validacao cruzada.
     """
    # TODO: separar treino/teste, definir Pipeline e grade de parametros,
    # ajustar CrossValidator e retornar (previsoes, cv_model, metricas).
    return None


def main() -> None:
    spark = criar_sessao("aula06-parte3-03-gold-manutencao")
    try:
        origem = SILVER / "sensores_maquinas"
        if not origem.exists():
            raise SystemExit(
                f"Silver ausente em {origem}. Rode os jobs 01 e 02 primeiro."
            )
        dados = spark.read.parquet(str(origem))
        titulo("Selecao de modelo: probabilidade de falha nas proximas 24h")
        previsoes, cv_model, metricas = treinar_modelo_manutencao(dados)

        previsoes.select(
            "leitura_id", "maquina_id", "falha_24h", "prediction",
            "probability",
        ).write.mode("overwrite").parquet(
            str(GOLD / "risco_falha_maquina")
        )
        cv_model.bestModel.write().overwrite().save(
            str(GOLD / "modelos" / "manutencao_gbt")
        )
        resultados = spark.createDataFrame(
            [
                ("auc_cv_media", float(max(cv_model.avgMetrics))),
                *metricas,
            ],
            ["metrica", "valor"],
        )
        resultados.coalesce(1).write.mode("overwrite").parquet(
            str(GOLD / "metricas")
        )
        print(
            f"AUC media CV={max(cv_model.avgMetrics):.3f} | "
            f"AUC teste={metricas[0][1]:.3f} | "
            f"recall teste={metricas[1][1]:.3f}"
        )
        print(f"Melhor modelo e previsoes gravados em {GOLD}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
