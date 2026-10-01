# Exercícios — aprendizado não supervisionado

Dois exercícios no Gold, sem coluna-alvo: primeiro agrupar clientes por comportamento, depois reduzir as features a duas componentes principais.

## Como resolver

```powershell
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
.\.venv\Scripts\python.exe 03_gold_nao_supervisionado.py
```

As funções Gold começam com `TODO`. Complete ambas e confira a quantidade de grupos, as saídas e o gabarito.

## Exercício 1 — segmentar perfis com K-Means

Complete `segmentar_clientes(dados)`.

1. Use estas features: `gasto_mensal`, `compras_mes`, `dias_desde_ultima_compra`, `visitas_mes`, `devolucoes_pct` e `gasto_por_compra`.
2. Encadeie `VectorAssembler`, `StandardScaler` e `KMeans(k=3, seed=42)` num `Pipeline`.
3. Ajuste em todos os dados Silver, gere os segmentos e obtenha `trainingCost` do modelo K-Means.
4. Retorne `(segmentos, modelo, wssse)`.

**Resposta esperada:** 900 clientes recebem um valor de `segmento` entre 0 e 2; as contagens dos três grupos somam 900. Não existe métrica de acurácia, porque não há rótulos verdadeiros.

<details>
<summary>Gabarito</summary>

```python
def segmentar_clientes(dados):
    assembler = VectorAssembler(
        inputCols=COLUNAS_FEATURE, outputCol="features_brutas"
    )
    scaler = StandardScaler(
        inputCol="features_brutas", outputCol="features",
        withMean=True, withStd=True,
    )
    kmeans = KMeans(
        featuresCol="features", predictionCol="segmento", k=3, seed=42
    )
    modelo = Pipeline(stages=[assembler, scaler, kmeans]).fit(dados)
    segmentos = modelo.transform(dados)
    wssse = modelo.stages[-1].summary.trainingCost
    return segmentos, modelo, float(wssse)
```

Padronizar é importante porque gasto em reais, visitas e porcentagem têm escalas distintas. O WSSSE mede distância interna aos clusters; ele tende a cair quando `k` aumenta e não diz sozinho qual segmentação é útil. Os IDs dos clusters são arbitrários.
</details>

---

## Exercício 2 — projetar os perfis com PCA

Complete `projetar_clientes(dados)`.

1. Use as mesmas features do exercício 1, sem qualquer coluna de cluster ou classe.
2. Monte um `Pipeline` com `VectorAssembler`, `StandardScaler` e `PCA(k=2, inputCol="features", outputCol="componentes")`.
3. Ajuste e transforme todos os dados Silver.
4. Retorne `(projecao, modelo)`.

**Resposta esperada:** 900 linhas com `cliente_id` e uma coluna vetorial `componentes`, que é convertida pelo job em `pc1` e `pc2`. A projeção não representa classes; pontos próximos têm atributos padronizados semelhantes.

<details>
<summary>Gabarito</summary>

```python
def projetar_clientes(dados):
    assembler = VectorAssembler(
        inputCols=COLUNAS_FEATURE, outputCol="features_brutas"
    )
    scaler = StandardScaler(
        inputCol="features_brutas", outputCol="features",
        withMean=True, withStd=True,
    )
    pca = PCA(k=2, inputCol="features", outputCol="componentes")
    modelo = Pipeline(stages=[assembler, scaler, pca]).fit(dados)
    projecao = modelo.transform(dados)
    return projecao, modelo
```

PCA projeta dados em eixos que explicam variação, não em dimensões que maximizem separação de classes. As duas componentes podem misturar atributos originais; use `explainedVariance` do estágio PCA para entender quanta variação ficou representada.
</details>
