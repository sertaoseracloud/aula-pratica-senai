# Exercício — o ETL de consumo de energia, do zero

Quatro exercícios, na ordem em que o pipeline processa o dado. É a mesma mecânica do [exercício de ETL com temperaturas](../PYSPARK-AZURE-BLOB/EXERCICIOS_AZURE.md) da pasta ao lado — só que aqui a nuvem é AWS (S3, emulado pelo floci) em vez de Azure, e o dado é consumo de energia por setor em dez cidades de Santa Catarina.

Todos os gabaritos foram conferidos em Python puro contra o dado gerado com a semente fixa do script — os números abaixo são a saída real, não uma estimativa.

## Como rodar

Antes de tudo, suba o floci e confira o ambiente (veja o [SETUP.md](SETUP.md) se ainda não fez isso):

```powershell
docker compose up -d --wait
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Depois, a cada mudança no arquivo:

```powershell
.\.venv\Scripts\python.exe 01_etl_energia_sc.py
```

Enquanto uma função não estiver completa, ela devolve `None`, e o script para com um `AttributeError` ou `TypeError` apontando exatamente onde parou. `00_conectar_s3.py` e as etapas de Extract/Load já vêm prontas: o exercício é só a parte do meio.

O ciclo é sempre o mesmo:

1. **Leia o enunciado** aqui e a *docstring* da função correspondente no script.
2. **Escreva o código** no lugar do `# TODO`.
3. **Rode o script inteiro** e compare com a "Resposta esperada".
4. **Só então** abra o bloco `Gabarito` — ele explica o *porquê*, não só o *como*.

---

## O dado

Leituras diárias de consumo de energia (kWh), um trimestre inteiro (jul–set/2024, 92 dias), em dez cidades de SC, separadas em quatro setores — residencial, comercial, industrial, rural. Colunas: `leitura_id`, `data`, `municipio`, `setor`, `consumo_kwh`, `unidades_consumidoras`, `tarifa_rs_kwh`. Gerada com semente fixa (`random.Random(19)`), com ~5% de sujeira plausível para um medidor de campo: município ausente ou em minúsculo, setor inválido, consumo negativo (erro de medição), tarifa fora de uma faixa plausível, e algumas duplicatas.

---

## 1. Normalizar

Na função `normalizar`, escreva:

1. `municipio` para maiúsculo e sem espaço nas pontas.
2. `setor` para **minúsculo** e sem espaço nas pontas (a lista `NOMES_SETORES` está em minúsculo — é o padrão de comparação aqui, diferente do `municipio`).
3. `data` de string para `date`.
4. `dropDuplicates` em `["leitura_id", "data", "municipio", "setor"]`.

**Resposta esperada:** `3680` linhas depois do dedup (13 duplicatas removidas de 3693 lidas).

<details>
<summary>Gabarito</summary>

```python
def normalizar(bruto: DataFrame) -> DataFrame:
    return (
        bruto
        .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
        .withColumn("setor", F.lower(F.trim(F.col("setor"))))
        .withColumn("data", F.to_date("data", "yyyy-MM-dd"))
        .dropDuplicates(["leitura_id", "data", "municipio", "setor"])
    )
```

Duas colunas de texto, duas convenções de caixa diferentes — `municipio` compara contra uma lista em maiúsculo, `setor` contra uma lista em minúsculo. Não existe uma regra universal de "sempre maiúsculo"; a convenção é definida por qual lista de referência você vai comparar, e o único erro real é ser inconsistente entre a normalização e a validação.
</details>

## 2. Validar

Na função `validar`, monte `regra_valida` como um `F.coalesce` de cinco condições com `&`:

- `municipio` está em `NOMES_MUNICIPIOS` (em maiúsculo);
- `setor` está em `NOMES_SETORES`;
- `consumo_kwh >= 0`;
- `tarifa_rs_kwh` está dentro de `[0.2, 2.0]`;
- `unidades_consumidoras > 0`.

E a coluna `motivo` nos rejeitados, com uma cadeia de `F.when` nesta ordem: município inválido, setor inválido, consumo negativo, tarifa fora da faixa, e o que sobrar é unidades consumidoras inválidas.

**Resposta esperada:** 3538 aprovadas, 142 rejeitadas.

```
setor invalido                                44
municipio invalida ou ausente                 35
tarifa fora da faixa (0.2 a 2.0)              35
consumo negativo                              28
unidades consumidoras invalidas                0
```

<details>
<summary>Gabarito</summary>

```python
def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    nomes_validos = [m.upper() for m in NOMES_MUNICIPIOS]

    regra_valida = F.coalesce(
        F.col("municipio").isin(nomes_validos)
        & F.col("setor").isin(NOMES_SETORES)
        & (F.col("consumo_kwh") >= 0)
        & F.col("tarifa_rs_kwh").between(0.2, 2.0)
        & (F.col("unidades_consumidoras") > 0),
        F.lit(False),
    )

    rejeitadas = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.when(~F.col("municipio").isin(nomes_validos), "municipio invalida ou ausente")
         .when(~F.col("setor").isin(NOMES_SETORES), "setor invalido")
         .when(F.col("consumo_kwh") < 0, "consumo negativo")
         .when(~F.col("tarifa_rs_kwh").between(0.2, 2.0), "tarifa fora da faixa (0.2 a 2.0)")
         .otherwise("unidades consumidoras invalidas"),
    )
    aprovadas = normalizado.filter(regra_valida)
    return aprovadas, rejeitadas
```

`unidades consumidoras invalidas` dá **zero** neste dado — a regra existe, mas o gerador nunca produz essa sujeira específica. Isso é intencional: nem toda regra de validação de um sistema real dispara sempre, e uma regra com contagem zero não é uma regra inútil, é uma regra que ainda não encontrou o dado que a justifica.
</details>

## 3. Enriquecer

Na função `enriquecer`, acrescente `custo_total` e `faixa_consumo`, depois junte com `municipios` para trazer a `regiao`:

- `custo_total`: `F.round(consumo_kwh * tarifa_rs_kwh, 2)`.
- `faixa_consumo`: `"baixo"` se `consumo_kwh < 5000`, `"medio"` se `<= 20000`, `"alto"` acima disso.
- `join(municipios, on="municipio", how="left")`.

**Resposta esperada:** 3538 linhas (mesma contagem das aprovadas). Faixas: alto 1653, médio 1260, baixo 625.

<details>
<summary>Gabarito</summary>

```python
def enriquecer(aprovadas: DataFrame, municipios: DataFrame) -> DataFrame:
    return (
        aprovadas
        .withColumn("custo_total", F.round(F.col("consumo_kwh") * F.col("tarifa_rs_kwh"), 2))
        .withColumn(
            "faixa_consumo",
            F.when(F.col("consumo_kwh") < 5000, "baixo")
             .when(F.col("consumo_kwh") <= 20000, "medio")
             .otherwise("alto"),
        )
        .join(municipios, on="municipio", how="left")
        .select(
            "leitura_id", "data", "municipio", "regiao", "setor",
            "consumo_kwh", "unidades_consumidoras", "tarifa_rs_kwh",
            "custo_total", "faixa_consumo",
        )
    )
```

A maioria das leituras cai em "alto" (1653 de 3538) porque o setor residencial e o industrial, nas cidades maiores, já passam de 20 000 kWh/dia sozinhos — o limiar de 5 000/20 000 kWh foi escolhido para o **consumo diário agregado por setor e cidade**, não por unidade consumidora. Se o objetivo fosse classificar o consumo de uma casa, os limiares certos seriam outros por três ordens de grandeza — vale sempre perguntar "por unidade de quê" antes de fixar um corte.
</details>

## 4. Resumir por região

Na função `resumir_por_regiao`, agrupe por `regiao` e calcule `leituras` (contagem), `consumo_medio_kwh` (média de `consumo_kwh`, 2 casas) e `custo_total_rs` (soma de `custo_total`, 2 casas). Ordene decrescente por `custo_total_rs`.

**Resposta esperada** (R$, trimestre inteiro):

```
Norte Catarinense        15698326.31
Grande Florianopolis     15259130.16
Vale do Itajai           14393176.59
Oeste Catarinense         4362617.22
Sul Catarinense           4354908.94
Serrana                   3174153.25
```

<details>
<summary>Gabarito</summary>

```python
def resumir_por_regiao(limpo: DataFrame) -> DataFrame:
    return (
        limpo.groupBy("regiao")
        .agg(
            F.count("*").alias("leituras"),
            F.round(F.avg("consumo_kwh"), 2).alias("consumo_medio_kwh"),
            F.round(F.sum("custo_total"), 2).alias("custo_total_rs"),
        )
        .orderBy(F.desc("custo_total_rs"))
    )
```

Note que a região Vale do Itajaí tem **mais leituras** que Norte Catarinense e Grande Florianópolis (ela reúne três cidades: Blumenau, Itajaí e Balneário Camboriú, contra uma cidade grande cada nas outras duas), mas fica em **terceiro** lugar em custo total — população agregada não é a mesma coisa que população concentrada numa cidade grande com indústria pesada. É o tipo de detalhe que some num `groupBy` sozinho e só aparece quando alguém pergunta "por que essa região, com mais leituras, gastou menos".
</details>

---

## Desafios, se sobrar fôlego

- **Compare com o exercício de temperaturas.** As funções `validar` das duas pastas usam a mesma estrutura (`F.coalesce` + cadeia de `F.when`), mas uma tem uma regra com contagem zero e a outra não. Rode os dois exercícios e liste as diferenças de projeto entre os dois pipelines — schema, número de regras, o que é `municipio` vs. o que é `setor`.
- **Troque a nuvem.** Depois de completar os quatro exercícios contra o floci, exporte `AWS_ENDPOINT_URL=""` com credenciais de uma conta AWS real (se você tiver uma) e rode de novo sem mudar nenhuma linha do script — veja a seção "Trocando o floci por uma conta AWS real" do [SETUP.md](SETUP.md).
- **Meça o custo do `cache()`.** Rode `limpo.count()` duas vezes, com e sem `limpo.cache()` logo após o exercício 3, e cronometre as duas.
