# Exercício — manutenção preditiva com validação cruzada

Um exercício no job Gold: comparar configurações de `GBTClassifier` por validação cruzada, escolher a melhor pelo AUC ROC médio e avaliar uma vez no conjunto de teste. Bronze e Silver estão completos.

## Como rodar

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
.\.venv\Scripts\python.exe 03_gold_manutencao.py
```

Enquanto `treinar_modelo_manutencao` retornar `None`, o Gold para ao tentar desempacotar o resultado. Leia a docstring, implemente a função e rode novamente. Depois, `executar_pipeline.py` roda as três camadas desde o início.

## O dado

O Bronze gera 2.400 leituras, cada uma com temperatura, vibração, pressão, horas desde a manutenção e carga da máquina. O rótulo `falha_24h` indica se a leitura está associada a uma falha simulada nas próximas 24 horas. Uma pequena quantidade de valores de sensor fora de faixa é inserida de propósito; a Silver grava essas linhas separadamente com `motivo`.

## Exercício — treinar e selecionar o estimador

Complete `treinar_modelo_manutencao(dados)` em `03_gold_manutencao.py`:

1. Separe treino e teste em 80/20, com `randomSplit([0.8, 0.2], seed=42)`.
2. Use `VectorAssembler` com `temperatura_c`, `vibracao_mm_s`, `pressao_bar`, `horas_desde_manutencao` e `carga_pct`. O rótulo é `falha_24h` e não deve estar nas features.
3. Monte um `Pipeline` com o assembler e `GBTClassifier(featuresCol="features", labelCol="falha_24h", seed=42)`.
4. Crie uma grade usando `ParamGridBuilder`: `maxDepth` em `[3, 5]` e `maxIter` em `[10, 20]`.
5. Configure `CrossValidator` com o pipeline, a grade, `BinaryClassificationEvaluator(labelCol="falha_24h", metricName="areaUnderROC")`, `numFolds=2`, `seed=42` e `parallelism=2`.
6. Ajuste o `CrossValidator` apenas no conjunto de treino. Transforme o teste com o `CrossValidatorModel` resultante.
7. Avalie no teste AUC ROC e recall da classe 1 com `MulticlassClassificationEvaluator(metricName="recallByLabel", metricLabel=1.0)`.
8. Retorne `(previsoes, cv_model, metricas)`, com `metricas` como pares `("auc_teste", valor)` e `("recall_teste", valor)`.

**Resposta esperada:** Silver concilia todas as leituras normalizadas entre aprovadas e rejeitadas. A previsão Gold contém `leitura_id`, `maquina_id`, `falha_24h`, `prediction` e `probability`. O modelo salvo é o `bestModel` selecionado no treino; as métricas incluem a melhor AUC média de validação cruzada e as métricas medidas no teste.

<details>
<summary>Gabarito</summary>

```python
def treinar_modelo_manutencao(dados):
    treino, teste = dados.randomSplit([0.8, 0.2], seed=42)
    assembler = VectorAssembler(
        inputCols=[
            "temperatura_c", "vibracao_mm_s", "pressao_bar",
            "horas_desde_manutencao", "carga_pct",
        ],
        outputCol="features",
    )
    classificador = GBTClassifier(
        featuresCol="features", labelCol="falha_24h", seed=42
    )
    pipeline = Pipeline(stages=[assembler, classificador])
    grade = (
        ParamGridBuilder()
        .addGrid(classificador.maxDepth, [3, 5])
        .addGrid(classificador.maxIter, [10, 20])
        .build()
    )
    avaliador = BinaryClassificationEvaluator(
        labelCol="falha_24h", metricName="areaUnderROC"
    )
    validacao = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grade,
        evaluator=avaliador,
        numFolds=2,
        seed=42,
        parallelism=2,
    )
    cv_model = validacao.fit(treino)
    previsoes = cv_model.transform(teste)
    auc_teste = avaliador.evaluate(previsoes)
    recall_teste = MulticlassClassificationEvaluator(
        labelCol="falha_24h", predictionCol="prediction",
        metricName="recallByLabel", metricLabel=1.0,
    ).evaluate(previsoes)
    metricas = [
        ("auc_teste", float(auc_teste)),
        ("recall_teste", float(recall_teste)),
    ]
    return previsoes, cv_model, metricas
```

`cv_model.avgMetrics` guarda uma métrica média por configuração da grade. Spark escolhe a configuração com melhor valor para o evaluator; o job grava `cv_model.bestModel`, não um pipeline recém-ajustado no teste. A validação cruzada custa mais que uma divisão única, então grades pequenas e paralelismo controlado são escolhas didáticas importantes.
</details>

## Desafio extra

Acrescente `maxDepth=7` à grade e compare o ganho de AUC média com o aumento do tempo. Depois, proponha uma divisão temporal: treinar em leituras antigas e testar em leituras posteriores, como seria mais realista para prever falhas futuras.
