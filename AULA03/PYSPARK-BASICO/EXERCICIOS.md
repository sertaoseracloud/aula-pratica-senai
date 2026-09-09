# Exercícios — do primeiro `filter` ao ETL

Doze exercícios em ordem crescente de dificuldade. Cada um traz **o enunciado**, **a resposta esperada** (o número que tem de sair) e **o gabarito comentado**.

Todos os gabaritos foram executados — os números abaixo são a saída real, não uma estimativa.

## Como rodar

O arquivo [`pratica.py`](pratica.py) já vem pronto na pasta, com o dado montado e uma função por exercício:

```powershell
.\.venv\Scripts\python.exe pratica.py 1       # roda so o exercicio 1
.\.venv\Scripts\python.exe pratica.py 1 4 8   # roda os exercicios 1, 4 e 8
.\.venv\Scripts\python.exe pratica.py         # roda todos
```

O **exercício 1 já vem resolvido**, como modelo. Rode-o primeiro para ver o formato da saída:

```
======================================================================
Exercicio 1
======================================================================
Pedidos com total acima de 1000, do maior para o menor.

+---------+-------+------+
|pedido_id|cliente| total|
+---------+-------+------+
|        1|    Ana|3600.0|
|        5|  Diego|2300.0|
|        6|  Carla|2300.0|
|        3|  Carla|1600.0|
+---------+-------+------+
```

Os demais imprimem `escreva a sua resposta em exercicio_N()` até você escrevê-la. O ciclo é:

1. **Leia o enunciado** aqui e a dica na *docstring* da função em `pratica.py`.
2. **Escreva o código** dentro da função, no lugar do `print(...)`.
3. **Rode só aquele exercício**: `python pratica.py 3`.
4. **Compare** com a "Resposta esperada" do enunciado.
5. **Só então** abra o bloco `Gabarito` aqui — ele explica o *porquê*, não só o *como*.

Cada execução leva ~15 s, quase tudo subindo a JVM. Por isso o arquivo aceita vários números de uma vez: resolva três ou quatro e rode-os juntos, em vez de um por execução.

O dado é o mesmo do `02_funcionalidades.py` — oito vendas, sendo a última sem `data_venda`, de propósito. `pratica.py` é seu: risque, quebre, experimente. Nenhum outro script do laboratório depende dele.

---

## Nível 1 — ler e filtrar

### 1. Os pedidos acima de mil reais

Mostre `pedido_id`, `cliente` e `total` apenas dos pedidos cujo **total** passou de R$ 1 000, do maior para o menor.

**Resposta esperada:** 4 pedidos — 3600.0, 2300.0, 2300.0, 1600.0.

<details>
<summary>Gabarito</summary>

```python
(
    vendas
    .filter(F.col("total") > 1000)
    .select("pedido_id", "cliente", "total")
    .orderBy(F.desc("total"))
    .show()
)
```

`orderBy(F.desc(...))` também pode ser escrito `orderBy(F.col("total").desc())`. As duas formas produzem o mesmo plano.
</details>

### 2. Livros vendidos em São Paulo

Quantos pedidos são da categoria `livros` **e** da UF `SP`?

**Resposta esperada:** `2`.

<details>
<summary>Gabarito</summary>

```python
print(vendas.filter((F.col("categoria") == "livros") & (F.col("uf") == "SP")).count())
```

O erro clássico aqui é escrever `and` no lugar de `&`:

```python
# NAO funciona: Column nao pode ser avaliada como booleano
vendas.filter(F.col("categoria") == "livros" and F.col("uf") == "SP")
```

Erro: `Cannot convert column into bool`. E os parênteses em volta de cada lado não são opcionais — `&` tem precedência maior que `==` em Python, então sem eles a expressão é avaliada na ordem errada.
</details>

### 3. A linha sem data

Mostre apenas o pedido cuja `data_venda` está ausente. Depois tente descobrir por que `vendas.filter(F.col("data_venda") != "2024-01-15")` **não** traz essa linha.

**Resposta esperada:** o pedido 8.

<details>
<summary>Gabarito</summary>

```python
vendas.filter(F.col("data_venda").isNull()).show()
```

Sobre a segunda parte: em SQL, qualquer comparação com `NULL` resulta em `NULL` — nunca em verdadeiro ou falso. `NULL != "2024-01-15"` é `NULL`, e o `filter` mantém apenas as linhas em que a condição é **verdadeira**. Resultado: a linha some do filtro sem que ninguém peça isso.

Para incluí-la explicitamente:

```python
vendas.filter((F.col("data_venda") != "2024-01-15") | F.col("data_venda").isNull()).show()
```

Este é o mesmo mecanismo que fez 505 linhas evaporarem do ETL — vale gastar tempo aqui.
</details>

---

## Nível 2 — agregar

### 4. Receita por categoria

Some o `total` por categoria, ordenando da maior receita para a menor. Use `cast("decimal(18,2)")` para não ver notação científica.

**Resposta esperada:**

```
eletronicos  6150.00
moveis       4600.00
livros        435.90
```

<details>
<summary>Gabarito</summary>

```python
(
    vendas.groupBy("categoria")
    .agg(F.sum("total").cast("decimal(18,2)").alias("receita"))
    .orderBy(F.desc("receita"))
    .show()
)
```
</details>

### 5. Quantos clientes distintos por UF

Conte clientes **distintos** por UF — não pedidos.

**Resposta esperada:** SP 2, RJ 2, MG 1.

<details>
<summary>Gabarito</summary>

```python
(
    vendas.groupBy("uf")
    .agg(
        F.count("*").alias("pedidos"),
        F.countDistinct("cliente").alias("clientes"),
    )
    .show()
)
```

Repare na diferença: SP tem **3** pedidos e **2** clientes, porque a Ana comprou duas vezes. Confundir `count` com `countDistinct` é a origem de metade dos relatórios errados que existem.
</details>

### 6. O maior pedido de cada cliente

Para cada cliente, o valor do maior pedido. Ordene pelo valor, decrescente.

**Resposta esperada:** Ana 3600.0, Carla 2300.0, Diego 2300.0, Bruno 950.0, Elisa 240.0.

<details>
<summary>Gabarito</summary>

```python
(
    vendas.groupBy("cliente")
    .agg(F.max("total").alias("maior_pedido"))
    .orderBy(F.desc("maior_pedido"))
    .show()
)
```

Cuidado com a pergunta seguinte, que parece igual e não é: *qual foi o maior pedido de cada cliente* (com `pedido_id`, data e categoria) não se responde com `groupBy` — `max` devolve o valor, não a linha. Isso é o exercício 8.
</details>

---

## Nível 3 — juntar e janelar

### 7. Vendas por região

Junte a tabela de regiões abaixo e some a receita por região.

```python
regioes = spark.createDataFrame(
    [("SP", "Sudeste"), ("RJ", "Sudeste"), ("MG", "Sudeste"), ("BA", "Nordeste")],
    "uf string, regiao string",
)
```

**Resposta esperada:** Sudeste 11185.90 — e nenhuma linha para o Nordeste.

<details>
<summary>Gabarito</summary>

```python
(
    vendas.join(regioes, on="uf", how="left")
    .groupBy("regiao")
    .agg(F.sum("total").cast("decimal(18,2)").alias("receita"))
    .show()
)
```

`on="uf"` (string, não `vendas.uf == regioes.uf`) faz o Spark manter **uma** coluna `uf` no resultado. Com a forma de expressão você fica com duas colunas de mesmo nome, e qualquer `select("uf")` depois disso falha com `Reference 'uf' is ambiguous`.

O `how="left"` preserva todas as vendas mesmo que a UF não exista na tabela de regiões. Trocar para `inner` aqui não muda o resultado, porque todas as UFs de venda estão na tabela — mas em produção é exatamente essa diferença que faz sumir faturamento de um relatório.
</details>

### 8. O maior pedido de cada cliente, com a linha inteira

Agora com `pedido_id`, `categoria` e `data_venda` do pedido campeão de cada cliente.

**Resposta esperada:** 5 linhas — uma por cliente, com o pedido de maior total.

<details>
<summary>Gabarito</summary>

```python
janela = Window.partitionBy("cliente").orderBy(F.desc("total"))
(
    vendas
    .withColumn("posicao", F.row_number().over(janela))
    .filter(F.col("posicao") == 1)
    .select("cliente", "pedido_id", "categoria", "total")
    .orderBy(F.desc("total"))
    .show()
)
```

Este é **o** padrão de "pegar a linha do máximo", e vale decorar: numera dentro do grupo com `row_number()` e fica com a posição 1.

`row_number()` sempre dá 1, 2, 3 sem empate; `rank()` repete a posição e pula a seguinte (1, 1, 3); `dense_rank()` repete sem pular (1, 1, 2). Se houver empate no seu dado e você usar `row_number`, o desempate é arbitrário — acrescente um critério ao `orderBy`.
</details>

### 9. Quanto cada pedido representa da sua UF

Acrescente a cada linha o percentual que aquele pedido representa da receita total da sua UF.

**Resposta esperada:** o pedido 1 (Ana, SP, 3600.0) é 90,23% de SP.

<details>
<summary>Gabarito</summary>

```python
por_uf = Window.partitionBy("uf")
(
    vendas
    .withColumn("total_uf", F.sum("total").over(por_uf))
    .withColumn("pct", F.round(F.col("total") / F.col("total_uf") * 100, 2))
    .select("pedido_id", "uf", "total", "total_uf", "pct")
    .orderBy("uf", F.desc("pct"))
    .show()
)
```

Sem `Window`, isso exigiria um `groupBy` seguido de um `join` de volta na tabela original — duas passadas e um shuffle a mais. É para isso que a função de janela existe: agregar **sem** colapsar as linhas.
</details>

---

## Nível 4 — o ETL

Os próximos exercícios são alterações no `03_etl_local.py`. Faça uma cópia antes de mexer.

### 10. Uma nova regra de validação

Rejeite também os pedidos com `preco` acima de R$ 2 900 (na base gerada eles existem e são plausíveis, mas suponha que a regra de negócio os considere suspeitos). Acrescente o motivo `"preco suspeito"` e confirme que a conciliação continua fechando.

**Resposta esperada:**

```
aprovadas .........: 46851
rejeitadas ........: 3149
conciliacao .......: OK

preco suspeito         1657
uf invalida ou ausente  505
data invalida           499
quantidade nao positiva  488
```

São 1 657 pedidos novos na lista de rejeitados — 3,3% da base. Um limiar de validação nunca é neutro: escolher 2 900 em vez de 3 000 muda quanto do seu faturamento vai parar na pasta de rejeitados.

<details>
<summary>Gabarito</summary>

Em `transformar`, acrescente a condição à regra:

```python
regra_valida = F.coalesce(
    F.col("pedido_id").isNotNull()
    & F.col("data_venda").isNotNull()
    & (F.col("uf").isin(UFS))
    & (F.col("quantidade") > 0)
    & (F.col("preco") > 0)
    & (F.col("preco") <= 2900),        # <- nova regra
    F.lit(False),
)
```

E o motivo correspondente, **antes** do `.otherwise`:

```python
.when(F.col("preco") > 2900, "preco suspeito")
```

O `assert` da conciliação é o que prova que você mexeu nos dois lugares. Se você alterar só a regra e esquecer o motivo, as linhas caem em `"outro"` — o total fecha, mas o relatório de rejeitados fica inútil. Se alterar só o motivo, nada muda. Os dois lados andam juntos, e é por isso que uma lista de regras costuma virar uma estrutura de dados única em código de produção.
</details>

### 11. Particionar por mês em vez de UF

Grave o fato particionado por `mes` e compare o número de arquivos e de pastas com a versão particionada por `uf`.

**Resposta esperada:** 12 pastas (uma por mês) em vez de 6.

<details>
<summary>Gabarito</summary>

```python
limpo.repartition("mes").write.mode("overwrite").partitionBy("mes").parquet(
    str(SAIDA / "vendas_por_mes")
)
```

A pergunta que decide entre `uf` e `mes` não é estética: é **por qual coluna a aplicação vai filtrar**. Um relatório mensal filtra por mês; um painel regional filtra por UF. Particionar pela coluna errada não dá erro nenhum — só faz toda consulta ler a base inteira.

Dá para particionar pelas duas (`partitionBy("uf", "mes")`), e aí são 6 × 12 = 72 pastas. Com 48 mil linhas isso já é excesso: cada pasta fica com ~670 linhas, e arquivos Parquet pequenos demais custam mais em metadados do que economizam em leitura.
</details>

### 12. Um relatório novo

Acrescente ao ETL um terceiro destino: o **ticket médio por cliente**, ordenado do maior para o menor, gravado em `saida/clientes/`. Só clientes com 60 pedidos ou mais.

**Resposta esperada:** 900 clientes na base (entre 30 e 78 pedidos cada), dos quais **194** passam no corte de 60.

<details>
<summary>Gabarito</summary>

Em `transformar`, junto ao `resumo`:

```python
clientes = (
    limpo.groupBy("cliente")
    .agg(
        F.count("*").alias("pedidos"),
        F.sum("total").cast("decimal(18,2)").alias("receita"),
        F.round(F.avg("total"), 2).alias("ticket_medio"),
    )
    .filter(F.col("pedidos") >= 60)
    .orderBy(F.desc("ticket_medio"))
)
```

E em `carregar`:

```python
clientes.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    str(SAIDA / "clientes")
)
```

Repare no `filter` **depois** do `agg`: ele filtra o resultado da agregação, e equivale ao `HAVING` do SQL. Um `filter` antes do `groupBy` seria o `WHERE` — filtraria linhas, não grupos. Trocar um pelo outro é um erro que não dá exceção, só número errado.
</details>

---

## Desafios, se sobrar fôlego

- **Cole `df.explain()` depois de um `filter`** sobre o Parquet gravado e procure `PushedFilters` na saída:

  ```
  FileScan parquet [...] DataFilters: [isnotnull(quantidade), (quantidade > 4)],
    PartitionFilters: [], PushedFilters: [IsNotNull(quantidade), GreaterThan(quantidade,4)]
  ```

  O filtro **desceu até a leitura do arquivo**: o Parquet guarda mínimo e máximo por bloco, então blocos inteiros são pulados sem serem lidos. Compare agora com um filtro por `uf`, que aparece em `PartitionFilters` — nesse caso nem o arquivo é aberto, porque a informação está no nome da pasta. São dois mecanismos diferentes, e os dois só funcionam com funções nativas: um filtro dentro de uma UDF é uma caixa-preta e não desce para lugar nenhum.
- **Meça `cache()`.** Rode `limpo.count()` duas vezes com e sem `limpo.cache()` antes, e cronometre as duas. Depois responda: por que a primeira execução com `cache()` não é mais rápida?
- **Quebre de propósito.** Escreva `F.col("uff")` no lugar de `F.col("uf")` e observe **quando** o erro aparece: na transformação ou só na ação seguinte? Compare com um erro de digitação dentro de uma UDF.
- **Compare `repartition(1)` com `coalesce(1)`** numa base de 48 mil linhas, cronometrando. Os dois produzem um arquivo só; um deles embaralha o dado inteiro pela rede para fazer isso.
