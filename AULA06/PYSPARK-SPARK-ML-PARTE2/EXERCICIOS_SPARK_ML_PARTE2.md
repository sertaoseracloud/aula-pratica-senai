# Exercícios — Spark ML, parte 2

Três exercícios novos no job Gold: classificação de fraude, análise de sentimento e recomendação colaborativa. Bronze e Silver estão prontos; implemente uma função `TODO` por vez em `03_gold_aplicacoes.py`.

## Como resolver

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
.\.venv\Scripts\python.exe 03_gold_aplicacoes.py
```

O Gold para no primeiro exercício incompleto, ao tentar desempacotar o `None`. Complete os três e execute `executar_pipeline.py` para percorrer todas as camadas desde a Bronze.

1. Leia a docstring da função e o enunciado abaixo.
2. Implemente a função no local do `TODO`.
3. Rode `03_gold_aplicacoes.py` e confira esquema, métricas e saídas Gold.
4. Consulte o gabarito depois da tentativa.

As sementes dos dados e da divisão treino/teste estão fixas. Contagens são reproduzíveis; métricas podem variar entre versões/configurações do Spark e devem ser interpretadas, não decoradas.

---

## Exercício 1 — detecção de fraude

Complete `detectar_fraudes(transacoes)`.

1. Use `randomSplit([0.8, 0.2], seed=42)`.
2. Monte o vetor `features` com `valor`, `hora`, `transacoes_24h`, `distancia_km` e `internacional`. `fraude` é a coluna-alvo e não pode entrar nas features.
3. Monte um `Pipeline` com `VectorAssembler` e `RandomForestClassifier(featuresCol="features", labelCol="fraude", numTrees=40, maxDepth=6, seed=42)`.
4. Ajuste no treino e transforme o teste.
5. Avalie com `BinaryClassificationEvaluator(metricName="areaUnderPR")` e `MulticlassClassificationEvaluator(metricName="recallByLabel", metricLabel=1.0)`.
6. Retorne `(previsoes, modelo, metricas)` com as tuplas `("fraude", "area_under_pr", valor)` e `("fraude", "recall", valor)`.

**Resposta esperada:** teste com `transacao_id`, `fraude`, `prediction` e `probability`; métricas entre 0 e 1; previsões gravadas em `camadas/gold/fraudes/` e modelo em `camadas/gold/modelos/fraude_rf/`. A fonte contém 1.800 transações e a classe `fraude=1` é minoritária.

<details>
<summary>Gabarito</summary>

```python
def detectar_fraudes(transacoes):
    treino, teste = transacoes.randomSplit([0.8, 0.2], seed=42)
    assembler = VectorAssembler(
        inputCols=[
            "valor", "hora", "transacoes_24h", "distancia_km",
            "internacional",
        ],
        outputCol="features",
    )
    floresta = RandomForestClassifier(
        featuresCol="features", labelCol="fraude", numTrees=40,
        maxDepth=6, seed=42,
    )
    modelo = Pipeline(stages=[assembler, floresta]).fit(treino)
    previsoes = modelo.transform(teste)
    auprc = BinaryClassificationEvaluator(
        labelCol="fraude", rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    ).evaluate(previsoes)
    recall = MulticlassClassificationEvaluator(
        labelCol="fraude", predictionCol="prediction",
        metricName="recallByLabel", metricLabel=1.0,
    ).evaluate(previsoes)
    metricas = [
        ("fraude", "area_under_pr", float(auprc)),
        ("fraude", "recall", float(recall)),
    ]
    return previsoes, modelo, metricas
```

AUPRC resume a relação precisão-recall e é mais informativa que acurácia quando positivos são raros. Recall responde: “de todas as fraudes reais, quantas o detector encontrou?”. Ele não considera sozinho o volume de alertas falsos; em operação, escolha o limiar considerando o custo de ambos os tipos de erro.
</details>

---

## Exercício 2 — sentimento em avaliações de texto

Complete `classificar_sentimento(avaliacoes)`.

1. Separe treino e teste usando a divisão 80/20 com `seed=42`.
2. Crie etapas em ordem: `RegexTokenizer(inputCol="texto", outputCol="tokens", pattern="\\W+")`, `StopWordsRemover(inputCol="tokens", outputCol="tokens_sem_stopwords")`, `HashingTF(inputCol="tokens_sem_stopwords", outputCol="features", numFeatures=256)` e `NaiveBayes(featuresCol="features", labelCol="sentimento", smoothing=1.0)`.
3. Encadeie as etapas num `Pipeline`, ajuste no treino e transforme o teste.
4. Avalie com `MulticlassClassificationEvaluator(metricName="accuracy")`.
5. Retorne `(previsoes, modelo, [("sentimento", "acuracia", valor)])`.

**Resposta esperada:** as previsões contêm `avaliacao_id`, `texto`, `sentimento` e `prediction`; 1.200 avaliações divididas aproximadamente entre as classes 0 e 1; acurácia superior a 0,8 neste corpus sintético. A saída fica em `camadas/gold/sentimentos/`.

<details>
<summary>Gabarito</summary>

```python
def classificar_sentimento(avaliacoes):
    treino, teste = avaliacoes.randomSplit([0.8, 0.2], seed=42)
    tokenizer = RegexTokenizer(
        inputCol="texto", outputCol="tokens", pattern="\\W+"
    )
    remover_stopwords = StopWordsRemover(
        inputCol="tokens", outputCol="tokens_sem_stopwords"
    )
    hashing = HashingTF(
        inputCol="tokens_sem_stopwords", outputCol="features",
        numFeatures=256,
    )
    classificador = NaiveBayes(
        featuresCol="features", labelCol="sentimento", smoothing=1.0
    )
    modelo = Pipeline(stages=[
        tokenizer, remover_stopwords, hashing, classificador
    ]).fit(treino)
    previsoes = modelo.transform(teste)
    acuracia = MulticlassClassificationEvaluator(
        labelCol="sentimento", predictionCol="prediction",
        metricName="accuracy",
    ).evaluate(previsoes)
    metricas = [("sentimento", "acuracia", float(acuracia))]
    return previsoes, modelo, metricas
```

`HashingTF` converte tokens em um vetor esparso de tamanho fixo. Essa representação pode ter colisões de hash; em corpora maiores, compare com `CountVectorizer` ou TF-IDF. A lista padrão de stop words é inglesa, mas os textos sintéticos foram construídos com palavras de conteúdo em português e sem acentos para manter o exemplo autocontido.
</details>

---

## Exercício 3 — recomendação colaborativa

Complete `recomendar_produtos(avaliacoes)`.

1. Divida as notas 80/20 com `seed=42`.
2. Ajuste `ALS` usando `userCol="usuario_id"`, `itemCol="produto_id"`, `ratingCol="nota"`, `rank=8`, `maxIter=10`, `regParam=0.1`, `coldStartStrategy="drop"` e `seed=42`.
3. Gere previsões para o teste e avalie `rmse` com `RegressionEvaluator`.
4. Retorne `(previsoes, modelo, [("recomendacao", "rmse", valor)])`.

**Resposta esperada:** previsões com `usuario_id`, `produto_id`, `nota` e `prediction`; recomendações Top-3 para usuários gravadas em `camadas/gold/recomendacoes_top3/`; RMSE positivo e modelo em `camadas/gold/modelos/als/`.

<details>
<summary>Gabarito</summary>

```python
def recomendar_produtos(avaliacoes):
    treino, teste = avaliacoes.randomSplit([0.8, 0.2], seed=42)
    als = ALS(
        userCol="usuario_id", itemCol="produto_id", ratingCol="nota",
        rank=8, maxIter=10, regParam=0.1,
        coldStartStrategy="drop", seed=42,
    )
    modelo = als.fit(treino)
    previsoes = modelo.transform(teste)
    rmse = RegressionEvaluator(
        labelCol="nota", predictionCol="prediction", metricName="rmse"
    ).evaluate(previsoes)
    metricas = [("recomendacao", "rmse", float(rmse))]
    return previsoes, modelo, metricas
```

ALS aprende vetores latentes para usuários e produtos a partir do padrão de notas, sem receber uma coluna explícita de “gosta deste grupo”. `coldStartStrategy="drop"` descarta pares do teste que não puderam ser previstos por falta de usuário ou produto no treino; por isso a quantidade de previsões pode ser menor que a quantidade de linhas de teste. O pipeline Gold usa `recommendForAllUsers(3)` para materializar três sugestões por usuário conhecido.
</details>

---

## Desafios extras

- Mude o limiar do Random Forest com `setThresholds` e examine como precisão e recall trocam de valor.
- Troque `numFeatures` no `HashingTF` e compare a acurácia e o tamanho dos vetores.
- Experimente `rank=4` e `rank=12` no ALS. Compare RMSE e variedade dos Top-3, sem tratar o menor erro offline como prova de maior satisfação real.
