# Exercícios — notas do ENEM em Santa Catarina

Catorze exercícios em ordem crescente de dificuldade, no mesmo formato do [`EXERCICIOS.md`](EXERCICIOS.md) original — só que agora o dado é uma simulação de notas do ENEM em dez cidades de Santa Catarina, e os últimos cinco exercícios constroem um ETL inteiro, não um script já pronto.

O dado **não é ENEM de verdade**: é gerado com semente fixa só para o exercício ter números para comparar. As cidades e as regiões são reais; candidatos, notas e inconsistências são sintéticos.

Todos os gabaritos foram executados — os números abaixo são a saída real, não uma estimativa.

## Como rodar

Os **níveis 1 a 3** usam [`pratica_enem.py`](pratica_enem.py), uma tabela pequena de 8 inscrições (igual ao `pratica.py` original com `vendas`):

```powershell
.\.venv\Scripts\python.exe pratica_enem.py 1       # roda so o exercicio 1
.\.venv\Scripts\python.exe pratica_enem.py 1 4 8   # roda os exercicios 1, 4 e 8
.\.venv\Scripts\python.exe pratica_enem.py         # roda todos
```

O **nível 4** (exercícios 10 a 14) é diferente: em vez de uma função por exercício, é **um script de ETL inteiro** — [`05_etl_enem_sc.py`](05_etl_enem_sc.py) — com cinco blocos marcados `# TODO Exercicio N`. Você edita esse arquivo diretamente e roda o script inteiro toda vez:

```powershell
.\.venv\Scripts\python.exe 05_etl_enem_sc.py
```

Na primeira execução ele gera sozinho `dados/enem_sc_bruto.csv` (8000 candidatos) e `dados/municipios_sc.csv` (a tabela de apoio com a região de cada cidade). Enquanto um TODO não for preenchido, a função devolve `None` e o script para com um `AttributeError` apontando exatamente o bloco que falta — é o sinal de que ainda há trabalho ali, não um bug escondido.

O **exercício 1 já vem resolvido**, como modelo:

```
======================================================================
Exercicio 1
======================================================================
Candidatos com media acima de 600, da maior para a menor.

+------------+---------+-----+
|inscricao_id|candidato|media|
+------------+---------+-----+
|           4|      Ana|690.0|
|           5|    Diego|630.0|
|           8|    Elisa|610.0|
+------------+---------+-----+
```

O ciclo é sempre o mesmo:

1. **Leia o enunciado** aqui e a dica na *docstring* da função (ou do bloco TODO).
2. **Escreva o código** no lugar indicado.
3. **Rode** e compare com a "Resposta esperada".
4. **Só então** abra o bloco `Gabarito` — ele explica o *porquê*, não só o *como*.

---

## Nível 1 — ler e filtrar

O dado de `pratica_enem.py`: oito inscrições, quatro cidades, uma delas (Elisa, pedido 8) sem `data_prova`, de propósito.

### 1. Candidatos com média acima de 600

Mostre `inscricao_id`, `candidato` e `media` apenas de quem tirou média acima de 600, da maior para a menor.

**Resposta esperada:** 3 candidatos — 690.0, 630.0, 610.0.

<details>
<summary>Gabarito</summary>

```python
(
    candidatos
    .filter(F.col("media") > 600)
    .select("inscricao_id", "candidato", "media")
    .orderBy(F.desc("media"))
    .show()
)
```
</details>

### 2. Inscrições de Joinville em escola privada

Quantas inscrições são do município `Joinville` **e** `tipo_escola` `privada`?

**Resposta esperada:** `3`.

<details>
<summary>Gabarito</summary>

```python
print(
    candidatos
    .filter((F.col("municipio") == "Joinville") & (F.col("tipo_escola") == "privada"))
    .count()
)
```

O mesmo erro clássico do exercício original: `and` do Python não funciona em cima de `Column` — usa `&`, e cada lado precisa dos parênteses porque `&` tem precedência maior que `==`.
</details>

### 3. A inscrição sem data de prova

Mostre apenas a inscrição cuja `data_prova` está ausente. Depois tente entender por que `candidatos.filter(F.col("data_prova") != "2024-01-15")` **não** traz 7 linhas.

**Resposta esperada:** a inscrição 8 (Elisa).

<details>
<summary>Gabarito</summary>

```python
candidatos.filter(F.col("data_prova").isNull()).show()
```

`NULL != "2024-01-15"` avalia para `NULL` em SQL, nunca para verdadeiro — e o `filter` só mantém linhas onde a condição é **verdadeira**. Das 8 linhas, 1 tem `data_prova` nula (some do filtro) e 1 tem exatamente essa data (não passa na condição), sobrando 6, não 7.
</details>

---

## Nível 2 — agregar

### 4. Média de nota_mt por tipo de escola

Some a `nota_mt` por `tipo_escola` (use `F.avg`, arredondado em 2 casas), da maior média para a menor.

**Resposta esperada:**

```
privada  603.33
publica  600.00
```

<details>
<summary>Gabarito</summary>

```python
(
    candidatos.groupBy("tipo_escola")
    .agg(F.round(F.avg("nota_mt"), 2).alias("media_mt"))
    .orderBy(F.desc("media_mt"))
    .show()
)
```
</details>

### 5. Quantos candidatos distintos por tipo de escola

Conte inscrições **e** candidatos **distintos** por `tipo_escola` — não é a mesma coisa: a Ana e a Carla aparecem duas vezes cada.

**Resposta esperada:** publica 5 inscrições / 3 candidatos, privada 3 inscrições / 2 candidatos.

<details>
<summary>Gabarito</summary>

```python
(
    candidatos.groupBy("tipo_escola")
    .agg(
        F.count("*").alias("inscricoes"),
        F.countDistinct("candidato").alias("candidatos"),
    )
    .show()
)
```

Confundir `count` com `countDistinct` é a origem de metade dos relatórios errados que existem — aqui ficaria mais visível ainda, porque a Ana sozinha responde por 2 das 5 inscrições públicas.
</details>

### 6. A maior nota_mt de cada candidato

Para cada candidato, o valor da maior `nota_mt`. Ordene decrescente.

**Resposta esperada:** Ana 680.0, Bruno 610.0, Diego 610.0 (empate), Elisa 600.0, Carla 560.0.

<details>
<summary>Gabarito</summary>

```python
(
    candidatos.groupBy("candidato")
    .agg(F.max("nota_mt").alias("melhor_mt"))
    .orderBy(F.desc("melhor_mt"))
    .show()
)
```

Bruno e Diego empatam em 610 — a ordem entre os dois não é garantida sem um critério de desempate extra no `orderBy`. Isso é o exercício 8, com a linha inteira em vez de só o valor.
</details>

---

## Nível 3 — juntar e janelar

### 7. Soma da média por região

Junte a tabela `regioes` (município → região) e some a coluna `media` por região.

```python
regioes = spark.createDataFrame(
    [
        ("Florianopolis", "Grande Florianopolis"),
        ("Joinville", "Norte Catarinense"),
        ("Blumenau", "Vale do Itajai"),
        ("Lages", "Serrana"),
    ],
    "municipio string, regiao string",
)
```

**Resposta esperada:** Grande Florianopolis 1900.0, Norte Catarinense 1830.0, Vale do Itajai 1050.0 — e **nenhuma linha** para Serrana (Lages não aparece em `candidatos`).

<details>
<summary>Gabarito</summary>

```python
(
    candidatos.join(regioes, on="municipio", how="left")
    .groupBy("regiao")
    .agg(F.sum("media").alias("soma_media"))
    .orderBy(F.desc("soma_media"))
    .show()
)
```

`on="municipio"` como string evita a coluna duplicada que apareceria com `candidatos.municipio == regioes.municipio`. E como Lages não tem nenhuma inscrição em `candidatos`, a região Serrana simplesmente não aparece no resultado — um `how="left"` a partir de `candidatos` nunca inventa linha que não existia do lado esquerdo.
</details>

### 8. A linha inteira da maior nota_mt de cada candidato

Agora com `inscricao_id`, `municipio` e `nota_mt` da inscrição campeã de cada candidato.

**Resposta esperada:** 5 linhas — uma por candidato.

<details>
<summary>Gabarito</summary>

```python
janela = Window.partitionBy("candidato").orderBy(F.desc("nota_mt"))
(
    candidatos
    .withColumn("posicao", F.row_number().over(janela))
    .filter(F.col("posicao") == 1)
    .select("candidato", "inscricao_id", "municipio", "nota_mt")
    .orderBy(F.desc("nota_mt"))
    .show()
)
```

O padrão de "pegar a linha do máximo": numerar dentro do grupo com `row_number()` e ficar com a posição 1. No empate do Bruno/Diego do exercício 6, aqui cada um é o único candidato do seu próprio grupo — o empate não afeta este resultado, só afetaria se dois candidatos com o mesmo nome disputassem a mesma partição.
</details>

### 9. Quanto cada inscrição representa da nota_mt da sua escola

Acrescente a cada linha o percentual que aquela `nota_mt` representa da soma de `nota_mt` do seu `tipo_escola`.

**Resposta esperada:** a inscrição 4 (Ana, pública, nota_mt 680) é 22.67% do total da pública (3000).

<details>
<summary>Gabarito</summary>

```python
por_tipo = Window.partitionBy("tipo_escola")
(
    candidatos
    .withColumn("total_tipo", F.sum("nota_mt").over(por_tipo))
    .withColumn("pct", F.round(F.col("nota_mt") / F.col("total_tipo") * 100, 2))
    .select("inscricao_id", "candidato", "tipo_escola", "nota_mt", "total_tipo", "pct")
    .orderBy("tipo_escola", F.desc("pct"))
    .show()
)
```

Sem `Window` isso pediria um `groupBy` seguido de um `join` de volta na tabela original. A função de janela agrega sem colapsar as linhas — é para isso que ela existe.
</details>

---

## Nível 4 — o ETL

Os cinco exercícios a seguir **são** o [`05_etl_enem_sc.py`](05_etl_enem_sc.py): um script de ETL real, com 8000 candidatos gerados a partir de `dados/enem_sc_bruto.csv`, e cinco funções de transformação incompletas. Abra o arquivo antes de continuar — cada função tem a *docstring* com o mesmo enunciado detalhado abaixo.

Rode o script inteiro a cada mudança:

```powershell
.\.venv\Scripts\python.exe 05_etl_enem_sc.py
```

### 10. Normalizar e remover duplicatas

Na função `normalizar`, escreva:

1. `municipio` para maiúsculo e sem espaço nas pontas.
2. `dropDuplicates` em `["inscricao_id", "ano", "municipio", "tipo_escola", "presente"]`.

**Resposta esperada:** `8000` linhas depois do dedup (36 duplicatas removidas de 8036 lidas).

<details>
<summary>Gabarito</summary>

```python
def normalizar(bruto: DataFrame) -> DataFrame:
    return (
        bruto
        .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
        .dropDuplicates(["inscricao_id", "ano", "municipio", "tipo_escola", "presente"])
    )
```

`F.upper(F.trim(...))` é o mesmo padrão do `uf` no `03_etl_local.py` — só que aqui o valor tem espaço (`"Jaragua do Sul"`), então o `upper` sozinho não bastaria para casar com a lista de municípios válidos se alguém tivesse digitado com espaço extra.
</details>

### 11. A regra de validação

Na função `validar`, monte `regra_valida` como um `F.coalesce` de quatro condições com `&`:

- `municipio` está em `NOMES_MUNICIPIOS` (compare em maiúsculo, já que `municipio` foi normalizado no exercício 10 — mas a lista `NOMES_MUNICIPIOS` está em texto normal, então compare com `F.upper(F.lit(m))` para cada item, ou monte a lista já em maiúsculo);
- `ano` está em `ANOS_VALIDOS`;
- se `presente == "S"`, `nota_lc` não é nula (senão a condição não importa — mas o `&` ainda precisa de um valor, não de erro);
- nenhuma das cinco notas está fora de `[0, 1000]` quando não é nula.

E a coluna `motivo` nos rejeitados, com uma cadeia de `F.when`.

**Resposta esperada:** 7659 aprovadas, 341 rejeitadas.

```
ano invalido                     91
nota fora da faixa 0-1000        89
municipio invalida ou ausente    89
notas ausentes para presente     72
```

<details>
<summary>Gabarito</summary>

```python
NOMES_MUNICIPIOS_UPPER = [m.upper() for m in NOMES_MUNICIPIOS]

def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    notas = ["nota_lc", "nota_ch", "nota_cn", "nota_mt", "nota_redacao"]

    faixa_ok = F.lit(True)
    for nota in notas:
        faixa_ok = faixa_ok & (F.col(nota).isNull() | F.col(nota).between(0, 1000))

    regra_valida = F.coalesce(
        F.col("municipio").isin(NOMES_MUNICIPIOS_UPPER)
        & F.col("ano").isin(ANOS_VALIDOS)
        & ~((F.col("presente") == "S") & F.col("nota_lc").isNull())
        & faixa_ok,
        F.lit(False),
    )

    rejeitados = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.when(~F.col("municipio").isin(NOMES_MUNICIPIOS_UPPER), "municipio invalida ou ausente")
         .when(~F.col("ano").isin(ANOS_VALIDOS), "ano invalido")
         .when((F.col("presente") == "S") & F.col("nota_lc").isNull(), "notas ausentes para presente")
         .otherwise("nota fora da faixa 0-1000"),
    )
    aprovados = normalizado.filter(regra_valida)
    return aprovados, rejeitados
```

A sujeira deste dado nunca acumula dois defeitos na mesma linha (cada linha suja tem exatamente um problema), então a ordem da cadeia `when` não muda a contagem final — mas em dado real isso não seria garantido, e a ordem passaria a importar (a primeira regra que bater "ganha" o motivo).

Sem o `F.coalesce(..., F.lit(False))`, uma linha com `presente` nulo (não existe neste dado, mas existiria em produção) faria a condição inteira virar `NULL`, e o `~regra_valida` de uma `NULL` também é `NULL` — a linha sumiria dos dois lados, aprovados e rejeitados, sem que a conciliação acusasse nada até você somar os dois totais.
</details>

### 12. Média geral e faixa de desempenho

Na função `enriquecer`, acrescente:

- `media_geral`: média das cinco notas, arredondada em 2 casas.
- `faixa_desempenho`: `"nao_compareceu"` se `presente == "N"`, senão `"baixo"` (< 500), `"medio"` (até 650) ou `"alto"` (acima de 650).

**Resposta esperada** (só entre quem compareceu, 6911 candidatos):

```
medio  5576
baixo  1292
alto     43
```

<details>
<summary>Gabarito</summary>

```python
def enriquecer(aprovados: DataFrame) -> DataFrame:
    media = (
        F.col("nota_lc") + F.col("nota_ch") + F.col("nota_cn")
        + F.col("nota_mt") + F.col("nota_redacao")
    ) / 5
    return (
        aprovados
        .withColumn("media_geral", F.round(media, 2))
        .withColumn(
            "faixa_desempenho",
            F.when(F.col("presente") == "N", "nao_compareceu")
             .when(F.col("media_geral") < 500, "baixo")
             .when(F.col("media_geral") <= 650, "medio")
             .otherwise("alto"),
        )
    )
```

O pulo do gato é a soma direta: se qualquer uma das cinco colunas for nula (candidato ausente), a soma inteira já sai nula em SQL — não precisa de `F.when` para tratar esse caso na média, só na faixa. É o mesmo mecanismo do `NULL != valor` do exercício 3, só que aqui jogando a favor em vez de contra.
</details>

### 13. Média geral por região

Na função `resumir_por_regiao`, junte `limpo` com `municipios` (município → região) e calcule a média de `media_geral` por região.

**Resposta esperada** (2 casas, decrescente):

```
Sul Catarinense          544.67
Norte Catarinense        543.56
Oeste Catarinense        543.03
Grande Florianopolis     540.89
Vale do Itajai           540.88
Serrana                  538.90
```

<details>
<summary>Gabarito</summary>

```python
def resumir_por_regiao(limpo: DataFrame, municipios: DataFrame) -> DataFrame:
    return (
        limpo.join(municipios, on="municipio", how="left")
        .groupBy("regiao")
        .agg(F.round(F.avg("media_geral"), 2).alias("media_regiao"))
        .orderBy(F.desc("media_regiao"))
    )
```

`F.avg` ignora `NULL` sozinho — os candidatos que não compareceram (com `media_geral` nula) não entram na conta, e não é preciso filtrá-los antes. Se a intenção fosse contar todo mundo, inclusive quem faltou, o jeito certo seria trocar a métrica, não forçar zero no lugar do nulo (isso derrubaria a média artificialmente).
</details>

### 14. Municípios em destaque

Na função `municipios_destaque`, agrupe por `municipio`, conte quantos candidatos `presente == "S"` cada um tem, e filtre **depois** do `groupBy` para manter só quem tem 700 ou mais.

**Resposta esperada:** 4 municípios.

```
SAO JOSE          730
FLORIANOPOLIS     718
JOINVILLE         714
BLUMENAU          710
```

<details>
<summary>Gabarito</summary>

```python
def municipios_destaque(limpo: DataFrame) -> DataFrame:
    return (
        limpo.groupBy("municipio")
        .agg(F.sum(F.when(F.col("presente") == "S", 1).otherwise(0)).alias("presentes"))
        .filter(F.col("presentes") >= 700)
        .orderBy(F.desc("presentes"))
    )
```

O `filter` depois do `agg` é o `HAVING` do SQL; um `filter(F.col("presente") == "S")` **antes** do `groupBy` seria o `WHERE` — mudaria a base da contagem, não o corte final. Os dois se parecem, mas um filtra linhas e o outro filtra grupos, e trocar um pelo outro aqui não daria erro nenhum, só um número de municípios errado.
</details>

---

## Desafios, se sobrar fôlego

- **Compare `Window` com `groupBy` + `join`.** Refaça o exercício 13 sem `Window` nem o `join` direto — primeiro um `groupBy("municipio")` para achar a média por cidade, depois um segundo `join` com `municipios` para agrupar por região. Duas passadas em vez de uma: cronometre a diferença numa base maior.
- **Quebre de propósito.** No exercício 11, troque `F.col("ano").isin(ANOS_VALIDOS)` por `F.col("ano").isin([2022, 2023, 2024])` escrito como string (`["2022", "2023", "2024"]`) e veja o que acontece com a contagem de `ano invalido` — o schema declara `ano` como `IntegerType`, então a comparação com string nunca bate.
- **Meça o efeito do `cache()`.** Rode `limpo.count()` duas vezes, com e sem `limpo.cache()` antes da linha do exercício 12, e cronometre. É o mesmo experimento do `EXERCICIOS.md` original, agora com 8000 linhas em vez de 48 mil — a diferença relativa muda?
