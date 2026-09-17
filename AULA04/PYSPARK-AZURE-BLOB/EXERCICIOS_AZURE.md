# Exercício — o ETL de temperaturas, do zero

Quatro exercícios, na ordem em que o pipeline processa o dado. É a mesma mecânica do [exercício de ETL com notas do ENEM](../PYSPARK-BASICO/EXERCICIOS_ENEM.md) da AULA03: você completa o [`01_etl_temperaturas_sc.py`](01_etl_temperaturas_sc.py) diretamente, uma função por vez, e roda o script inteiro a cada mudança.

Todos os gabaritos foram conferidos em Python puro contra o dado gerado com a semente fixa do script — os números abaixo são a saída real, não uma estimativa.

## Como rodar

Antes de tudo, suba o floci-az e confira o ambiente (veja o [SETUP.md](SETUP.md) se ainda não fez isso):

```powershell
docker compose up -d --wait
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Depois, a cada mudança no arquivo:

```powershell
.\.venv\Scripts\python.exe 01_etl_temperaturas_sc.py
```

Enquanto uma função não estiver completa, ela devolve `None`, e o script para com um `AttributeError` ou `TypeError` apontando exatamente onde parou — não é um bug escondido, é o sinal de qual bloco falta. `00_conectar_blob.py` e as etapas de Extract/Load já vêm prontas: o exercício é só a parte do meio, a que não muda seja qual for a nuvem.

O ciclo é sempre o mesmo:

1. **Leia o enunciado** aqui e a *docstring* da função correspondente no script.
2. **Escreva o código** no lugar do `# TODO`.
3. **Rode o script inteiro** e compare com a "Resposta esperada".
4. **Só então** abra o bloco `Gabarito` — ele explica o *porquê*, não só o *como*.

---

## 1. Normalizar

Na função `normalizar`, escreva:

1. `municipio` para maiúsculo e sem espaço nas pontas.
2. `data` de string para `date`, com `F.to_date("data", "yyyy-MM-dd")`.
3. `dropDuplicates` em `["estacao_id", "data", "municipio"]`.

**Resposta esperada:** `920` linhas depois do dedup (4 duplicatas removidas de 924 lidas).

<details>
<summary>Gabarito</summary>

```python
def normalizar(bruto: DataFrame) -> DataFrame:
    return (
        bruto
        .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
        .withColumn("data", F.to_date("data", "yyyy-MM-dd"))
        .dropDuplicates(["estacao_id", "data", "municipio"])
    )
```

`F.upper(F.trim(...))` é o mesmo padrão do `uf` no `03_etl_local.py` da AULA03 e do `municipio` no exercício de ENEM — a diferença aqui é que o valor tem espaço (`"Jaragua do Sul"`), então o `upper` sozinho não bastaria se alguém tivesse digitado com espaço extra na ponta.
</details>

## 2. Validar

Na função `validar`, monte `regra_valida` como um `F.coalesce` de quatro condições com `&`:

- `municipio` está em `NOMES_MUNICIPIOS` (em maiúsculo, já que `municipio` foi normalizado no exercício 1);
- `temperatura_min <= temperatura_max`;
- `temperatura_min` e `temperatura_max` estão dentro de `[-15, 50]`;
- `umidade_pct` está dentro de `[0, 100]`.

E a coluna `motivo` nos rejeitados, com uma cadeia de `F.when` nesta ordem: município inválido, depois `min > max`, depois faixa física, e o que sobrar é umidade inválida.

**Resposta esperada:** 886 aprovadas, 34 rejeitadas.

```
temperatura_min maior que temperatura_max      11
municipio invalida ou ausente                   8
umidade fora da faixa (0 a 100)                 8
temperatura fora da faixa fisica (-15 a 50)     7
```

<details>
<summary>Gabarito</summary>

```python
def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    nomes_validos = [m.upper() for m in NOMES_MUNICIPIOS]

    regra_valida = F.coalesce(
        F.col("municipio").isin(nomes_validos)
        & (F.col("temperatura_min") <= F.col("temperatura_max"))
        & F.col("temperatura_min").between(-15, 50)
        & F.col("temperatura_max").between(-15, 50)
        & F.col("umidade_pct").between(0, 100),
        F.lit(False),
    )

    rejeitadas = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.when(~F.col("municipio").isin(nomes_validos), "municipio invalida ou ausente")
         .when(F.col("temperatura_min") > F.col("temperatura_max"), "temperatura_min maior que temperatura_max")
         .when(
             ~F.col("temperatura_min").between(-15, 50) | ~F.col("temperatura_max").between(-15, 50),
             "temperatura fora da faixa fisica (-15 a 50)",
         )
         .otherwise("umidade fora da faixa (0 a 100)"),
    )
    aprovadas = normalizado.filter(regra_valida)
    return aprovadas, rejeitadas
```

Sem o `F.coalesce(..., F.lit(False))`, uma linha com `municipio` nulo faria `isin(...)` avaliar para `NULL`, e `~NULL` também é `NULL` — a linha sumiria dos dois lados (aprovadas e rejeitadas) sem que a conciliação notasse, até você somar os dois totais e sobrar uma diferença muda.

Das 11 leituras rejeitadas por `min > max`, 7 **também** violam a faixa física — são casos em que o sensor grava `temperatura_max` como `-25 °C` mantendo o `temperatura_min` original. A ordem do `F.when` decide que o motivo registrado é `min > max`, não faixa física, porque essa condição vem primeiro na cadeia.
</details>

## 3. Enriquecer

Na função `enriquecer`, acrescente `amplitude_termica` e `faixa_dia`, depois junte com `municipios` para trazer a `regiao`:

- `amplitude_termica`: `F.round(temperatura_max - temperatura_min, 1)`.
- `faixa_dia`: `"fria"` se `temperatura_media < 15`, `"amena"` se `<= 22`, `"quente"` acima disso.
- `join(municipios, on="municipio", how="left")`.

**Resposta esperada:** 886 linhas (mesma contagem das aprovadas), cada uma com `regiao` preenchida.

<details>
<summary>Gabarito</summary>

```python
def enriquecer(aprovadas: DataFrame, municipios: DataFrame) -> DataFrame:
    return (
        aprovadas
        .withColumn("amplitude_termica", F.round(F.col("temperatura_max") - F.col("temperatura_min"), 1))
        .withColumn(
            "faixa_dia",
            F.when(F.col("temperatura_media") < 15, "fria")
             .when(F.col("temperatura_media") <= 22, "amena")
             .otherwise("quente"),
        )
        .join(municipios, on="municipio", how="left")
        .select(
            "estacao_id", "data", "municipio", "regiao",
            "temperatura_min", "temperatura_max", "temperatura_media",
            "amplitude_termica", "umidade_pct", "faixa_dia",
        )
    )
```

`on="municipio"` como string (não `aprovadas.municipio == municipios.municipio`) evita a coluna duplicada — o mesmo detalhe do exercício 7 do laboratório de vendas/regiões da AULA03. Como toda leitura aprovada tem um município da lista válida, o `how="left"` aqui nunca perde linha nem gera `regiao` nula — mas continua sendo a escolha certa por hábito: um `inner` teria o mesmo resultado só porque o dado já está limpo neste ponto do pipeline.
</details>

## 4. Resumir por região

Na função `resumir_por_regiao`, agrupe por `regiao` e calcule `leituras` (contagem), `media_trimestral` (média de `temperatura_media`, 2 casas) e `amplitude_media` (média de `amplitude_termica`, 2 casas). Ordene decrescente por `media_trimestral`.

**Resposta esperada** (°C, decrescente):

```
Grande Florianopolis     19.72
Vale do Itajai           19.49
Norte Catarinense        19.01
Sul Catarinense          18.21
Oeste Catarinense        17.66
Serrana                  14.66
```

<details>
<summary>Gabarito</summary>

```python
def resumir_por_regiao(limpo: DataFrame) -> DataFrame:
    return (
        limpo.groupBy("regiao")
        .agg(
            F.count("*").alias("leituras"),
            F.round(F.avg("temperatura_media"), 2).alias("media_trimestral"),
            F.round(F.avg("amplitude_termica"), 2).alias("amplitude_media"),
        )
        .orderBy(F.desc("media_trimestral"))
    )
```

A Serrana (Lages) sai quase 5 graus abaixo da região mais quente — plausível para quem conhece o inverno na serra catarinense, e uma checagem rápida de que o dado sintético (e o seu código) não saíram absurdos.
</details>

---

## Desafios, se sobrar fôlego

- **Quebre de propósito.** No exercício 2, troque a ordem do `F.when` — coloque a checagem de faixa física antes de `min > max` — e rode de novo. A contagem total de rejeitadas continua 34, mas a distribuição por motivo muda. Explique por quê usando as 7 linhas que violam as duas regras ao mesmo tempo.
- **Meça o custo do `cache()`.** Rode `limpo.count()` duas vezes, com e sem `limpo.cache()` logo após o exercício 3, e cronometre as duas.
- **Troque a nuvem.** Depois de completar os quatro exercícios contra o floci-az, exporte `AZURE_STORAGE_CONNECTION_STRING` apontando para outra instância do emulador (ou para uma conta Azure real, se você tiver uma) e rode de novo sem mudar nenhuma linha do script — veja a seção "Trocando o floci-az por uma conta Azure real" do [SETUP.md](SETUP.md).
