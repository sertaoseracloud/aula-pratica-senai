# Exercícios — aprendizado supervisionado

Dois exercícios no Gold. Ambos têm uma coluna-alvo explícita; cada enunciado pede para separar treino e teste, ajustar um pipeline e avaliar previsões no teste.

## Como resolver

```powershell
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
.\.venv\Scripts\python.exe 03_gold_supervisionado.py
```

O Gold para no primeiro `TODO`. Implemente cada função, execute novamente e compare o resultado esperado antes de abrir o gabarito.

---

## Exercício 1 — classificação de cancelamento

Complete `classificar_churn(dados)` em `03_gold_supervisionado.py`.

1. Separe treino e teste em 80/20 com `randomSplit([0.8, 0.2], seed=42)`.
2. Monte features com `tenure_meses`, `mensalidade`, `chamados_suporte` e `contrato_meses`. O alvo é `cancelou`; não o inclua nas features.
3. Encadeie `VectorAssembler` e `LogisticRegression(featuresCol="features", labelCol="cancelou", maxIter=40, regParam=0.05)` em um `Pipeline`.
4. Ajuste no treino e transforme o teste.
5. Calcule `areaUnderROC` e `accuracy`; retorne previsões, modelo e as métricas como tuplas `(caso, metrica, valor)`.

**Resposta esperada:** a saída contém apenas o conjunto de teste, com `cliente_id`, alvo, `prediction` e `probability`. AUC e acurácia ficam entre 0 e 1; o modelo e as previsões são gravados na Gold.

<details>
<summary>Gabarito</summary>

```python
def classificar_churn(dados):
    treino, teste = dados.randomSplit([0.8, 0.2], seed=42)
    assembler = VectorAssembler(
        inputCols=[
            "tenure_meses", "mensalidade", "chamados_suporte",
            "contrato_meses",
        ],
        outputCol="features",
    )
    classificador = LogisticRegression(
        featuresCol="features", labelCol="cancelou", maxIter=40,
        regParam=0.05,
    )
    modelo = Pipeline(stages=[assembler, classificador]).fit(treino)
    previsoes = modelo.transform(teste)
    auc = BinaryClassificationEvaluator(
        labelCol="cancelou", metricName="areaUnderROC"
    ).evaluate(previsoes)
    acuracia = MulticlassClassificationEvaluator(
        labelCol="cancelou", metricName="accuracy"
    ).evaluate(previsoes)
    metricas = [
        ("churn", "area_under_roc", float(auc)),
        ("churn", "accuracy", float(acuracia)),
    ]
    return previsoes, modelo, metricas
```

Classificação prevê uma classe discreta. A AUC mede a ordenação entre positivos e negativos; a acurácia mede a proporção de previsões corretas no limiar padrão.
</details>

---

## Exercício 2 — regressão da demanda

Complete `prever_demanda(dados)`.

1. Separe treino e teste em 80/20 com `seed=42`.
2. Use `temperatura_c`, `dia_semana`, `feriado` e `ocupacao_pct` como features. O alvo contínuo é `demanda_mwh`.
3. Use `VectorAssembler` e `RandomForestRegressor(featuresCol="features", labelCol="demanda_mwh", numTrees=40, maxDepth=6, seed=42)` num `Pipeline`.
4. Ajuste no treino, preveja o teste e calcule RMSE e MAE.
5. Retorne previsões, modelo e métricas como tuplas `(caso, metrica, valor)`.

**Resposta esperada:** previsões com `registro_id`, demanda observada e `prediction`; RMSE e MAE positivos, na unidade MWh.

<details>
<summary>Gabarito</summary>

```python
def prever_demanda(dados):
    treino, teste = dados.randomSplit([0.8, 0.2], seed=42)
    assembler = VectorAssembler(
        inputCols=[
            "temperatura_c", "dia_semana", "feriado", "ocupacao_pct",
        ],
        outputCol="features",
    )
    regressor = RandomForestRegressor(
        featuresCol="features", labelCol="demanda_mwh", numTrees=40,
        maxDepth=6, seed=42,
    )
    modelo = Pipeline(stages=[assembler, regressor]).fit(treino)
    previsoes = modelo.transform(teste)
    rmse = RegressionEvaluator(
        labelCol="demanda_mwh", metricName="rmse"
    ).evaluate(previsoes)
    mae = RegressionEvaluator(
        labelCol="demanda_mwh", metricName="mae"
    ).evaluate(previsoes)
    metricas = [
        ("energia", "rmse_mwh", float(rmse)),
        ("energia", "mae_mwh", float(mae)),
    ]
    return previsoes, modelo, metricas
```

Regressão prevê um número. RMSE dá peso maior a erros grandes; MAE é o erro absoluto médio. As duas métricas usam a mesma unidade do alvo.
</details>
