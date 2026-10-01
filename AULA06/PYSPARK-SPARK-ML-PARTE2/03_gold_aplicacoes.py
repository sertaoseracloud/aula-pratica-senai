"""Treina modelos para fraude, sentimento e recomendacao na camada Gold."""

from __future__ import annotations

from comum import GOLD, SILVER, criar_sessao, titulo
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.feature import (
    HashingTF,
    RegexTokenizer,
    StopWordsRemover,
    VectorAssembler,
)
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F


def detectar_fraudes(transacoes):
    """Exercicio 1 -- classificar transacoes com fraude como classe rara.

     Nesta ordem:
     1. Divida transacoes em treino e teste (80/20), com seed=42.
     2. Monte features com valor, hora, transacoes_24h, distancia_km e
         internacional. `fraude` e o rotulo, nao uma feature.
     3. Use VectorAssembler e RandomForestClassifier (numTrees=40,
         maxDepth=6, seed=42) em um Pipeline; ajuste no treino.
     4. Transforme o teste e avalie areaUnderPR e recallByLabel para a classe
         1.0.
     5. Retorne (previsoes, modelo, metricas), com metricas no formato
         ("fraude", nome_metrica, valor).

     Resposta esperada: previsoes com transacao_id, fraude, prediction e
     probability; AUPRC e recall entre 0 e 1. A fonte tem 1800 transacoes,
     e as fraudes sao a classe minoritaria.
     """
    # TODO Exercicio 1: montar features, treinar RandomForestClassifier,
    # prever no teste e retornar (previsoes, modelo, metricas).
    return None


def classificar_sentimento(avaliacoes):
    """Exercicio 2 -- classificar sentimento a partir do texto da avaliacao.

     Nesta ordem:
     1. Divida avaliacoes em treino e teste (80/20), com seed=42.
     2. Monte um Pipeline com RegexTokenizer (pattern="\\W+"),
         StopWordsRemover, HashingTF (numFeatures=256) e NaiveBayes
         (smoothing=1.0).
     3. Ajuste o pipeline no treino e transforme o teste.
     4. Avalie accuracy e retorne (previsoes, modelo, metricas), com a tupla
         ("sentimento", "acuracia", valor).

     Resposta esperada: previsoes com avaliacao_id, texto, sentimento e
     prediction; acuracia acima de 0.8 neste corpus sintetico.
     """
    # TODO Exercicio 2: tokenizar, remover stop words, criar HashingTF,
    # treinar NaiveBayes, avaliar e retornar (previsoes, modelo, metricas).
    return None


def recomendar_produtos(avaliacoes):
    """Exercicio 3 -- estimar notas e recomendar produtos por ALS.

     Nesta ordem:
     1. Divida as notas em treino e teste (80/20), com seed=42.
     2. Ajuste ALS com usuario_id, produto_id e nota como colunas de entrada;
         use rank=8, maxIter=10, regParam=0.1, seed=42 e
         coldStartStrategy="drop".
     3. Transforme o teste e avalie RMSE com RegressionEvaluator.
     4. Retorne (previsoes, modelo, metricas), com a tupla
         ("recomendacao", "rmse", valor).

     Resposta esperada: previsoes com usuario_id, produto_id, nota e
     prediction; o job Gold grava tambem as tres recomendacoes por usuario.
     """
    # TODO Exercicio 3: dividir dados, treinar ALS, prever notas no teste,
    # avaliar RMSE e retornar (previsoes, modelo, metricas).
    return None


def salvar_metricas(spark, linhas: list[tuple[str, str, float]]) -> None:
    metricas = spark.createDataFrame(linhas, ["caso_uso", "metrica", "valor"])
    metricas.coalesce(1).write.mode("overwrite").parquet(
        str(GOLD / "metricas")
    )


def main() -> None:
    spark = criar_sessao("aula06-parte2-03-gold")
    resultados: list[tuple[str, str, float]] = []
    try:
        titulo("Classificacao: deteccao de fraude")
        transacoes = spark.read.parquet(str(SILVER / "transacoes"))
        previsoes, modelo, metricas = detectar_fraudes(transacoes)
        resultados.extend(metricas)
        previsoes.select(
            "transacao_id", "fraude", "prediction", "probability"
        ).write.mode("overwrite").parquet(str(GOLD / "fraudes"))
        modelo.write().overwrite().save(str(GOLD / "modelos" / "fraude_rf"))
        print(f"AUPRC={metricas[0][2]:.3f} | recall={metricas[1][2]:.3f}")

        titulo("Classificacao de texto: sentimento de avaliacoes")
        avaliacoes = spark.read.parquet(str(SILVER / "avaliacoes"))
        previsoes, modelo, metricas = classificar_sentimento(avaliacoes)
        resultados.extend(metricas)
        previsoes.select(
            "avaliacao_id", "texto", "sentimento", "prediction"
        ).write.mode("overwrite").parquet(str(GOLD / "sentimentos"))
        modelo.write().overwrite().save(
            str(GOLD / "modelos" / "sentimento_nb")
        )
        print(f"acuracia={metricas[0][2]:.3f}")

        titulo("Recomendacao: produtos para cada usuario")
        notas = spark.read.parquet(str(SILVER / "avaliacoes_produto"))
        previsoes, modelo, metricas = recomendar_produtos(notas)
        resultados.extend(metricas)
        previsoes.select(
            "usuario_id", "produto_id", "nota", "prediction"
        ).write.mode("overwrite").parquet(str(GOLD / "notas_previstas"))
        recomendacoes = modelo.recommendForAllUsers(3)
        recomendacoes.write.mode("overwrite").parquet(
            str(GOLD / "recomendacoes_top3")
        )
        modelo.write().overwrite().save(str(GOLD / "modelos" / "als"))
        print(f"RMSE das notas={metricas[0][2]:.3f}")
        recomendacoes.show(5, truncate=False)

        salvar_metricas(spark, resultados)
        print(f"\nResultados e modelos persistidos em {GOLD}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
