# Laboratório 8 — PySpark do zero: DataFrame, funcionalidades e um ETL local

**Duração: ~10 minutos** (~2 min preparando o ambiente + ~4 min rodando os quatro scripts + leitura).

Este laboratório é para quem **nunca abriu o Spark**. Ele não pressupõe cluster, Docker, Hadoop, nem experiência prévia com processamento distribuído — só Python e alguma familiaridade com SQL. Tudo roda na sua máquina, e a saída vai para uma pasta no seu disco.

| Script | O que mostra | Tempo medido |
| --- | --- | --- |
| [`00_primeiro_contato.py`](00_primeiro_contato.py) | o menor programa PySpark possível, linha por linha | **41 s** |
| [`01_dataframe.py`](01_dataframe.py) | seis formas de criar um DataFrame e o que cada uma decide | **55 s** |
| [`02_funcionalidades.py`](02_funcionalidades.py) | select, filter, nulos, `groupBy`, join, `Window`, SQL, partição, UDF | **99 s** |
| [`03_etl_local.py`](03_etl_local.py) | ETL completo: CSV bruto → validação → Parquet particionado | **27 s** |

E, para praticar: **[12 exercícios com gabarito comentado](EXERCICIOS.md)**, todos executados e conferidos.

> **Por que os scripts demoram tanto para tão pouco dado?** Quase todo o tempo é a JVM subindo (~5 s por sessão) e o Spark montando e agendando tarefas. Com oito linhas de dado, o overhead é praticamente 100% do tempo. É esperado: o Spark foi feito para dez milhões de linhas, não para oito — e, se você medir "o Spark é lento" com um dado pequeno, vai medir só o custo de partida.

---

## Sumário

1. [Antes do código: o vocabulário mínimo](#antes-do-código-o-vocabulário-mínimo)
2. [O que você vai descobrir](#o-que-você-vai-descobrir)
3. [Instalação](#instalação) — o guia completo está em [SETUP.md](SETUP.md)
4. [Passo a passo para rodar](#passo-a-passo-para-rodar)
5. [Script 00 — primeiro contato](#script-00--primeiro-contato)
6. [Script 01 — criar um DataFrame](#script-01--criar-um-dataframe)
7. [Script 02 — as funcionalidades](#script-02--as-funcionalidades)
8. [Script 03 — o ETL](#script-03--o-etl)
9. [Glossário](#glossário)
10. [Como ler um erro do Spark](#como-ler-um-erro-do-spark)
11. [Se algo der errado](#se-algo-der-errado)
12. [O que levar disso para o trabalho](#o-que-levar-disso-para-o-trabalho)

---

## Antes do código: o vocabulário mínimo

Sete termos. Eles aparecem em toda mensagem de erro do Spark, e entender os sete resolve boa parte da confusão inicial.

**Spark** é um motor que executa o mesmo cálculo sobre pedaços diferentes do dado ao mesmo tempo. Você escreve o programa uma vez; ele divide o dado e roda o cálculo em paralelo. O código que você escreve aqui, em uma máquina, é **o mesmo** que roda num cluster de cem — muda a configuração, não o programa.

**Driver** é o seu processo Python: o que você iniciou com `python 00_primeiro_contato.py`. Ele monta o plano, coordena o trabalho e recebe os resultados. É onde o `print` imprime.

**Executor** é quem faz o trabalho. Neste laboratório o *master* é `local[4]`, ou seja: quatro tarefas em paralelo dentro do próprio processo. Num cluster, seriam outras máquinas.

**Partição** é um pedaço do dado. O Spark roda **uma tarefa por partição** — é essa a unidade de paralelismo. Partição de mais: muita tarefa minúscula, e o custo de agendar supera o de calcular. Partição de menos: núcleos parados. É o botão que você mais vai girar na vida real.

**DataFrame** é uma tabela com esquema (nomes e tipos de coluna), dividida em partições. Ele **não guarda os dados na memória do seu Python** — guarda a receita de como obtê-los.

**Transformação × ação** é a distinção mais importante de todas:

| | Exemplos | O que acontece |
| --- | --- | --- |
| **Transformação** | `select`, `filter`, `withColumn`, `groupBy`, `join`, `orderBy` | nada executa; um plano é acumulado |
| **Ação** | `show`, `count`, `collect`, `take`, `write` | o plano acumulado é otimizado e executado |

Isso se chama **avaliação preguiçosa** (*lazy*), e tem uma consequência prática incômoda: um erro na sua transformação às vezes só estoura muitas linhas depois, na ação seguinte.

**Shuffle** é a redistribuição do dado entre partições — o que acontece num `groupBy`, num `join` ou num `repartition`. É a operação cara do Spark, porque envolve serializar e mover dado entre tarefas (e, num cluster, pela rede). Quando algo está lento em Spark, a primeira pergunta é quantos shuffles o plano tem.

**Catalyst** é o otimizador. Ele reescreve o seu plano antes de executar: reordena filtros, elimina colunas que ninguém usa, empurra condições para a leitura do arquivo. É por isso que **o Spark não executa o que você escreveu** — executa o que o Catalyst decidiu, e `df.explain()` é a única forma de ver o quê.

---

## O que você vai descobrir

### 1. Um `NULL` numa condição de validação faz linhas sumirem sem erro nenhum

A primeira versão do ETL separava aprovados de rejeitados assim:

```python
aprovados  = normalizado.filter(regra_valida)
rejeitados = normalizado.filter(~regra_valida)     # o "resto", certo?
```

Não. Em SQL — e o Spark é SQL por baixo —, `NULL` negado continua `NULL`, e `filter` só mantém as linhas em que a condição é **verdadeira**. As 505 linhas com UF ausente davam `NULL` na regra, `NULL` na negação, e não entravam em nenhum dos dois lados:

```
aprovadas ..: 48508
rejeitadas ..:   987      <- faltavam 505
```

Nenhum log, nenhuma exceção, nenhum aviso. O ETL "funcionou". Quem pegou isso foi uma linha de conciliação:

```python
assert aprovadas + rejeitadas == lidas
```

Custa uma contagem, e é a checagem mais barata que um pipeline pode ter. A correção está no script:

```python
regra_valida = F.coalesce(<a expressão>, F.lit(False))   # NULL vira falso
```

### 2. O Spark não executa o que você escreveu

O script 02 mede a mesma UDF de dois jeitos:

```
nativa F.upper()              340 ms
UDF Python                   4427 ms      -> 13x mais lenta
UDF + count()                 121 ms      <- não mediu nada
```

Na terceira linha a UDF **não rodou**. `count()` não usa a coluna calculada, então o Catalyst apagou a projeção inteira do plano — e a medição saiu rapidíssima porque nada foi calculado. É a forma mais comum de "provar" uma otimização que nunca aconteceu.

**Regra que fica:** para medir uma transformação, a ação precisa consumir o resultado dela.

### 3. Uma linha antes do `write` decide quantos arquivos você deposita

```
repartition(4)        24 arquivos em 6 pastas
repartition('uf')      6 arquivos em 6 pastas
```

Quatro partições em memória × seis UFs = 24 arquivos. É o "problema dos arquivos pequenos", e ele nasce exatamente aí: **partições em memória × valores distintos da coluna de particionamento**.

---

## Instalação

O guia completo, com o que instalar, por quê, e o que fazer quando cada coisa falha, está em **[SETUP.md](SETUP.md)**. O resumo, para quem já tem Python 3.11 e Java:

```powershell
cd AULA03\PYSPARK-BASICO

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" pandas pyarrow

# so no Windows: o Hadoop precisa destes dois ate para gravar em disco local
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll   $R/hadoop.dll

.\.venv\Scripts\python.exe verificar_ambiente.py
```

Três exigências que costumam pegar quem está começando, e que o `verificar_ambiente.py` checa por você:

- **Python 3.8 a 3.12.** O PySpark 3.5 não roda em 3.13 nem 3.14 — a instalação funciona, a execução não.
- **Java 8, 11 ou 17.** O Spark é JVM; sem Java, o erro fala de um gateway do py4j e não menciona Java em lugar nenhum.
- **`winutils.exe` no Windows.** Sim, mesmo sem S3 e gravando só em disco local. Sem ele a sessão nem sobe.

## Passo a passo para rodar

```powershell
.\.venv\Scripts\python.exe 00_primeiro_contato.py
.\.venv\Scripts\python.exe 01_dataframe.py
.\.venv\Scripts\python.exe 02_funcionalidades.py
.\.venv\Scripts\python.exe 03_etl_local.py
```

Estas duas linhas aparecem no começo de toda execução e **são normais** — não são erro:

```
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
```

Para reexecutar o ETL do zero, apagando a saída mas preservando a entrada:

```powershell
.\.venv\Scripts\python.exe 03_etl_local.py --limpar
```

> As pastas `.venv/` (590 MB), `hadoop/`, `dados/` e `saida/` estão no `.gitignore` — são dependências e artefatos, não código do laboratório.

### O que há em cada arquivo

| Arquivo | Papel |
| --- | --- |
| `comum.py` | cria a `SparkSession` e ajusta o ambiente (o interpretador dos workers, o `HADOOP_HOME`). Toda a infraestrutura mora aqui, para que os exemplos fiquem só com o PySpark |
| `00`–`03` | os exemplos, em ordem crescente |
| `verificar_ambiente.py` | confere Python, PySpark, Java e `winutils` em 1 s, sem subir sessão |
| `SETUP.md` | o guia de instalação completo |
| `EXERCICIOS.md` | 12 exercícios com gabarito |
| `pratica.py` | onde você resolve os exercícios; roda um, vários ou todos |
| `dados/` | a entrada do ETL, gerada na primeira execução |
| `saida/` | o que o ETL grava |

---

## Script 00 — primeiro contato

Vinte linhas úteis: criar a sessão, montar um DataFrame de três pessoas, filtrar, executar, criar uma coluna, trazer o resultado de volta.

**A sessão** é o ponto de entrada de tudo, e criá-la sobe a JVM — é o passo lento de qualquer script Spark:

```python
spark = criar_sessao("aula03-00-primeiro-contato")   # ~5 s
```

Num script de produção você cria **uma** sessão e a reaproveita; criar várias é um erro comum de quem está começando.

**A surpresa central** está nas seções 3 e 4:

```
3. Transformacao: o Spark anota, mas nao executa
o filter voltou instantaneamente e NAO leu nenhuma linha.
o que existe agora e um plano: DataFrame

4. Acao: a execucao acontece aqui
+-----+-----+---+
| nome|idade| uf|
+-----+-----+---+
|  Ana|   34| SP|
|Carla|   45| MG|
+-----+-----+---+
```

E há um detalhe de Python que morde todo mundo uma vez:

```python
F.col("idade") >= 30
```

Isso **não** devolve `True` nem `False` — devolve um objeto `Column` que *descreve* a comparação. Por isso `and`, `or` e `not` do Python não funcionam em condições de DataFrame:

```python
df.filter((F.col("a") > 1) & (F.col("b") == "x"))     # certo
df.filter(F.col("a") > 1 and F.col("b") == "x")       # erro
```

```
[CANNOT_CONVERT_COLUMN_INTO_BOOL] Cannot convert column into bool:
please use '&' for 'and', '|' for 'or', '~' for 'not'
```

Os parênteses também não são opcionais: `&` tem precedência maior que `>` em Python.

**Sobre `collect()`:** ele traz **tudo** para a memória do driver. Com três linhas é inofensivo; com dez milhões, derruba o processo com `OutOfMemory`. Para espiar o conteúdo use `show(5)` ou `take(5)`.

**Imutabilidade:** `withColumn` não altera o DataFrame original — devolve um novo. O script prova isso imprimindo as colunas de `pessoas` depois de criar `com_faixa`.

---

## Script 01 — criar um DataFrame

Seis formas de criar, e a diferença entre elas é **quem define os tipos**:

| Forma | Quem decide o tipo | Quando usar |
| --- | --- | --- |
| lista de tuplas + nomes | o Spark, inferindo dos valores | rascunho, teste rápido |
| string DDL (`"id int, nome string"`) | você, em uma linha | o caso comum |
| `StructType` explícito | você, incluindo **nulabilidade** | leitura de arquivo, contrato de dado |
| `Row` / dicionários | o Spark | quando o dado já vem nesse formato |
| `spark.range(n)` | fixo (`bigint`) | gerar volume para medir |
| a partir de um pandas | o pandas | trazer um dado pequeno de fora |

Declarar o esquema não é preciosismo. Sem ele, ler um arquivo grande custa **uma passada inteira só para descobrir os tipos** — e uma única linha suja transforma a coluna toda em string, silenciosamente.

Repare também nesta saída:

```
tipos .......: [('id', 'bigint'), ('nome', 'string'), ...]
```

O `1` que você escreveu em Python virou `bigint`, não `int`. O Spark tem o **seu próprio** sistema de tipos; os tipos Python são convertidos na fronteira, e é por isso que `IntegerType` e `LongType` são coisas diferentes lá dentro.

A seção 7 mostra o que olhar antes de transformar qualquer coisa: `columns`, `dtypes`, `count()`, `rdd.getNumPartitions()` e `describe()`. A seção 9 imprime o plano físico:

```
== Physical Plan ==
*(1) Filter (isnotnull(valor#3) AND (valor#3 > 2000.0))
+- *(1) Scan ExistingRDD[id#0L,nome#1,uf#2,valor#3]
```

Repare no `isnotnull(valor)` que **você não escreveu**: o Catalyst o acrescentou, porque uma comparação com `NULL` nunca é verdadeira e a linha seria descartada de qualquer jeito. Ler planos é uma habilidade que se constrói olhando muitos; comece por este.

---

## Script 02 — as funcionalidades

Dez seções sobre oito linhas de venda. As que mais mordem, em ordem:

### Nulos

A linha 8 tem `data_venda` ausente, e ela existe no dado justamente para isto: comparar com `NULL` não dá falso — dá `NULL`. Um filtro `col != 'x'` descarta em silêncio todas as linhas nulas, e ninguém percebe até o total não bater. As ferramentas são `isNull()`, `isNotNull()`, `fillna()`, `dropna()` e `coalesce()`.

### `groupBy` × `Window`

As duas agregam. `groupBy` colapsa N linhas em uma; `Window` mantém as N e anexa o agregado ao lado:

```
| uf|cliente| total|posicao_na_uf|total_da_uf|
| SP|    Ana|3600.0|            1|     3990.0|
| SP|  Elisa| 240.0|            2|     3990.0|
| SP|    Ana| 150.0|            3|     3990.0|
```

O padrão `row_number().over(janela)` + `filter(posicao == 1)` é como se pega **a linha inteira** do máximo de cada grupo — `max()` devolve o valor, não a linha. Vale decorar: aparece em toda base de código que processa dados.

### Dinheiro em `double`

`SUM` de ponto flutuante vira notação científica assim que a base cresce, e acumula erro de arredondamento. `cast("decimal(18,2)")` resolve — e é o mesmo motivo pelo qual dinheiro não se guarda em `double` em banco nenhum.

### Partição

`repartition` embaralha (shuffle) e pode aumentar ou diminuir; `coalesce` só junta partições existentes, sem shuffle, e por isso **só diminui**. A saída da seção 9 tem uma surpresa:

```
linhas por particao apos repartition('uf'): [8]
```

Foram criadas três partições (uma por UF) e o **AQE** (*adaptive query execution*, ligado por padrão no Spark 3) juntou as três, por serem minúsculas. O Spark olhou o tamanho real em tempo de execução e decidiu que o shuffle não valia a pena. Em volume real elas permanecem separadas. Este é um bom lembrete de que o plano que executa não é só o que você escreveu, nem só o que o Catalyst planejou — é o que o AQE ajustou vendo o dado.

### UDF

Uma UDF (*user-defined function*) é uma função Python sua aplicada linha a linha. O custo tem duas partes: cada linha precisa ser serializada do formato interno da JVM para Python e de volta; e o Catalyst não enxerga o que há dentro dela — para o otimizador, é uma caixa-preta que impede reordenar filtros ou empurrá-los para a leitura.

```
nativa F.upper()              340 ms
UDF Python                   4427 ms
```

Treze vezes, aqui. Num Linux o fator costuma ser menor (~3×), porque lá o Spark reaproveita um *daemon* de workers Python e no Windows sobe um processo por tarefa. **A conclusão não muda com o sistema operacional; o número, sim.**

Antes de escrever uma UDF, procure a nativa equivalente — `pyspark.sql.functions` cobre muito mais do que parece. Quando não der, `pandas_udf` processa em lote via Arrow e recupera boa parte da diferença.

---

## Script 03 — o ETL

**ETL** é *extract, transform, load*: ler de uma fonte, tratar, gravar num destino. O script separa as três etapas em funções distintas de propósito — é assim que se testa cada uma isoladamente.

```
dados/vendas_brutas.csv          50 245 linhas, 2,5 MB, geradas com semente fixa
      ↓ extract     schema declarado, mode PERMISSIVE
      ↓ transform   normaliza, deduplica, valida, enriquece
saida/vendas/uf=SP/*.parquet     48 508 linhas aprovadas, 6 arquivos
saida/resumo_mensal/             agregado por UF, mês e categoria (1 arquivo)
saida/rejeitados/                1 492 linhas, com o motivo de cada uma
```

### A entrada

É gerada pelo próprio script na primeira execução, com **4% de sujeira** do tipo que aparece em arquivo real: UF ausente, UF em minúscula, quantidade negativa, data em outro formato e duplicatas exatas. A semente do gerador é fixa (`random.Random(42)`), então os números deste README se repetem na sua máquina.

Se o arquivo já existe, ele **não** é regerado — é o que torna o script repetível.

### Extract

```python
spark.read.option("header", "true").option("mode", "PERMISSIVE").schema(SCHEMA_BRUTO).csv(...)
```

O `mode` decide o que fazer com uma linha que não casa com o esquema:

| Modo | Comportamento |
| --- | --- |
| `PERMISSIVE` (padrão) | põe `NULL` no campo ruim e segue |
| `DROPMALFORMED` | descarta a linha inteira, calado |
| `FAILFAST` | aborta o job |

Aqui queremos **ver** o que entrou errado, então ficamos com o padrão e validamos depois. `DROPMALFORMED` é sedutor e perigoso: ele resolve o sintoma escondendo o problema.

### Transform

Quatro etapas, nesta ordem:

1. **Normalizar** — `upper(trim(uf))` conserta `" sp "` e `"sp"`; `to_date` converte a string em data e devolve `NULL` quando o formato não casa (é por isso que `"31/02/2024"` vira nulo em vez de derrubar o job).
2. **Deduplicar** — `dropDuplicates` sobre as colunas que definem a identidade do registro. 245 linhas saem aqui.
3. **Validar** — a regra separa aprovados de rejeitados, e **os rejeitados são gravados com o motivo**, não descartados.
4. **Enriquecer** — colunas derivadas (`total`, `ano`, `mes`, `faixa_valor`) que o consumidor não deveria ter de recalcular.

**Rejeitar não é descartar.** Um pipeline que apenas filtra o inválido não consegue responder *o que foi perdido e por quê* — e essa pergunta sempre chega, geralmente de alguém que percebeu que o número do relatório não bate:

```
|motivo                 |count|
|uf invalida ou ausente |505  |
|data invalida          |499  |
|quantidade nao positiva|488  |
```

**A conciliação** fecha a etapa:

```
lidas .............: 50245
apos dropDuplicates: 50000  (-245 duplicatas)
aprovadas .........: 48508
rejeitadas ........: 1492
conciliacao .......: OK (aprovadas + rejeitadas = lidas apos dedup)
```

Foi ela que revelou o bug do `NULL`. Uma linha de `assert`.

### Load

```python
limpo.repartition("uf").write.mode("overwrite").partitionBy("uf").parquet(destino)
```

Três decisões nessa linha:

**`mode("overwrite")`** é o que torna o ETL repetível. O padrão é `errorIfExists`, que falha na segunda execução — e "rodar de novo" é a coisa mais comum que se faz com um pipeline.

**`partitionBy("uf")`** cria a estrutura de pastas no padrão Hive:

```
saida/vendas/uf=SP/part-00000-....snappy.parquet
saida/vendas/uf=RJ/part-00001-....snappy.parquet
```

**`repartition("uf")`** controla quantos arquivos saem. A seção 4 do script grava a mesma tabela nos dois layouts e conta:

```
repartition(4)        24 arquivos em 6 pastas
repartition('uf')      6 arquivos em 6 pastas
```

Quatro partições em memória × seis UFs = 24. Com uma partição por UF, sai um arquivo por pasta.

> Neste laboratório o CSV cabe num bloco só, então a entrada já chega com **uma** partição e a escrita daria 6 arquivos de qualquer jeito. O `repartition("uf")` está no script porque o efeito multiplicativo aparece assim que a entrada cresce — e aí o estrago já foi feito.

**Por que Parquet e não CSV?** Parquet é colunar (lê só as colunas pedidas), comprimido (~10× menor que CSV aqui) e carrega o esquema junto — tipos não precisam ser adivinhados na leitura. Guarda também mínimo e máximo por bloco, o que permite pular blocos inteiros num filtro. Os rejeitados vão para CSV de propósito: são para humanos abrirem.

### Verificação

```
colunas ..........: [..., 'total', 'faixa_valor', 'uf']
leitura completa .....    122 ms -> 48508 linhas
filtro uf='SP' .......    122 ms ->  8081 linhas
```

`uf` volta **por último**, embora estivesse no meio na escrita: ela não está gravada dentro dos arquivos — o Spark a reconstrói a partir do nome da pasta. **A coluna de particionamento deixa de ser dado e vira metadado do caminho.**

O filtro por `uf` não lê as outras UFs (*partition pruning*). Aqui a diferença de tempo é pequena porque a base inteira cabe em 1 MB; o ganho aparece quando cada partição é um arquivo de verdade. O critério para escolher a coluna de particionamento é sempre o mesmo: **poucos valores distintos, e presentes no `WHERE`** da aplicação. Particionar por `pedido_id` criaria 48 mil pastas com um arquivo cada.

---

## Glossário

| Termo | O que é |
| --- | --- |
| **ação** | operação que dispara a execução (`show`, `count`, `collect`, `write`) |
| **AQE** | *adaptive query execution*: o Spark reajusta o plano vendo o tamanho real do dado |
| **Catalyst** | o otimizador que reescreve o seu plano antes de executar |
| **driver** | o seu processo Python; coordena e recebe resultados |
| **executor** | quem executa as tarefas; em `local[4]`, threads do próprio processo |
| **lazy** | avaliação preguiçosa: transformações não executam até uma ação pedir |
| **partição** | pedaço do dado; uma tarefa por partição |
| **partition pruning** | pular pastas inteiras num filtro pela coluna de particionamento |
| **Parquet** | formato colunar comprimido, com esquema embutido |
| **predicate pushdown** | empurrar o filtro para a leitura do arquivo (`PushedFilters` no plano) |
| **shuffle** | redistribuição do dado entre partições; a operação cara |
| **skew** | partições de tamanhos muito diferentes; uma tarefa segura o job inteiro |
| **transformação** | operação que só monta plano (`select`, `filter`, `join`) |
| **UDF** | função Python aplicada linha a linha; opaca ao otimizador |

---

## Como ler um erro do Spark

O *traceback* de um erro de PySpark costuma ter 60 linhas, das quais 55 são a pilha interna da JVM. Três regras ajudam:

**1. Leia de baixo para cima, procurando `Caused by:`.** A causa real quase sempre está na última ocorrência de `Caused by:`, não no topo.

**2. Ignore a pilha do py4j.** Linhas com `py4j.protocol`, `java.lang.reflect` e `SparkSubmit` são o encanamento entre Python e JVM. A informação está antes ou depois delas.

**3. Localize a ação, não a linha do traceback.** Como as transformações são preguiçosas, a linha Python apontada é a da **ação** (`show`, `count`, `write`) — o erro pode ter sido introduzido dezenas de linhas antes. Quando isso acontecer, use `df.explain()` ou vá comentando transformações até isolar.

Um exemplo real deste laboratório:

```
java.lang.RuntimeException: java.io.FileNotFoundException:
HADOOP_HOME and hadoop.home.dir are unset.
```

A primeira linha (`RuntimeException`) não diz nada; a segunda resolve o problema. É sempre assim.

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| `ModuleNotFoundError: No module named 'pyspark'` | está rodando o Python do sistema, não o do `.venv` | use `.\.venv\Scripts\python.exe` |
| `HADOOP_HOME and hadoop.home.dir are unset` | falta `winutils.exe` (só Windows) | [SETUP.md, Passo 4](SETUP.md) |
| `winutils.exe` com 470 bytes | o CDN devolveu uma página de erro | apagar `hadoop/bin` e repetir o [Passo 4](SETUP.md) |
| erro de gateway do py4j ao criar a sessão | não há Java no `PATH` | instalar JDK 8, 11 ou 17 |
| a sessão sobe mas o Python é 3.13/3.14 | PySpark 3.5 não suporta | recriar o `.venv` com 3.11 ou 3.12 |
| `Python worker failed to connect back` | o worker subiu com outro Python | `comum.py` já fixa `PYSPARK_PYTHON`; rodar de novo |
| `CANNOT_CONVERT_COLUMN_INTO_BOOL` | usou `and`/`or` em vez de `&`/`\|` | `(cond1) & (cond2)`, com parênteses |
| `Reference 'uf' is ambiguous` | join que manteve a coluna dos dois lados | `join(outro, on="uf")` em vez da forma com `==` |
| `AnalysisException: cannot resolve 'coluna'` | nome de coluna errado | `df.columns` mostra os nomes reais |
| a soma sai como `4.9E7` | `sum` de `double` | `cast("decimal(18,2)")` |
| linhas somem entre entrada e saída | `NULL` numa condição de validação | `F.coalesce(regra, F.lit(False))` + conciliação |
| a UDF parece mais rápida que a nativa | `count()` fez o otimizador apagar a projeção | agregar a coluna calculada |
| muitos arquivos pequenos na saída | partições em memória × valores da coluna | `repartition("col")` antes do `write` |
| `PermissionError` ao regravar `saida/` | algum processo está com o arquivo aberto | fechar o processo e usar `--limpar` |
| a segunda execução falha com "already exists" | faltou `mode("overwrite")` | acrescentar o modo na escrita |

---

## O que levar disso para o trabalho

**Toda linha que entra tem de sair de algum lado.** Aprovada ou rejeitada, mas contabilizada. A conciliação custa uma contagem e pega a classe de erro mais silenciosa que existe em ETL — a linha que some porque uma condição virou `NULL`.

**Rejeitado é dado, não lixo.** Gravar os reprovados com o motivo transforma "o número não bate" numa consulta de dois minutos, em vez de uma investigação de dois dias.

**Declarar o esquema é mais barato que inferir.** Uma passada a menos no arquivo, e o tipo deixa de depender do conteúdo das primeiras linhas.

**A partição responde a duas perguntas diferentes.** *Quantas tarefas quero em paralelo?* governa `repartition(n)`. *Por qual coluna a aplicação vai filtrar?* governa `partitionBy("col")`. Responder uma com a outra é o que gera tanto o desbalanceamento quanto o excesso de arquivos.

**Funções nativas não são preferência de estilo.** São as únicas que o Catalyst enxerga — e por isso as únicas que ele consegue reordenar, empurrar para a leitura ou eliminar.

**Antes de confiar num número de performance, confirme que a ação consome o resultado.** Na dúvida, `df.explain()` mostra o plano que vai executar de fato — e não o que você escreveu.

---

**Próximo passo:** os [12 exercícios](EXERCICIOS.md), do primeiro `filter` até alterar o ETL. Os seis primeiros se resolvem em uma linha cada.

Você os resolve em [`pratica.py`](pratica.py), que já vem com o dado montado e o exercício 1 respondido como modelo:

```powershell
.\.venv\Scripts\python.exe pratica.py 1     # roda so o exercicio 1
```
