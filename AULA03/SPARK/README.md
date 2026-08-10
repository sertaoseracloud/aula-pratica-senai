# Laboratório 8 — Spark: do DataFrame ao S3

**Duração: ~5 minutos** (5 s subindo o floci + ~2 min preparando o ambiente local + 144 s rodando o notebook).

## O que você vai fazer

Rodar um pipeline PySpark inteiro no VS Code, com o kernel na sua própria máquina: criar o DataFrame no código, controlar as partições, transformar com as funções do dia a dia e gravar Parquet particionado num S3 local.

Só o S3 fica em Docker. O Spark roda no seu Python.

O notebook `aula03-spark.ipynb` **vem no clone** e roda do começo ao fim sem edição.

## O que você vai descobrir

**O Spark não executa o que você escreveu. Ele executa o plano que reescreveu.**

Isso soa como detalhe de implementação até você tentar medir alguma coisa. Neste laboratório, uma medição aparentemente correta mostra que uma UDF em Python é **mais rápida** que a função nativa equivalente:

```
nativa + count()    mediana=193 ms
UDF    + count()    mediana=185 ms
```

O resultado é falso, e o motivo é o otimizador: como `count()` não usa as colunas calculadas, ele apagou a projeção inteira do plano. Nenhuma das duas funções chegou a rodar.

Forçando o cálculo, a diferença real aparece:

```
nativa: F.upper()        mediana=411 ms
UDF Python: c.upper()    mediana=5534 ms      -> 13,5x
```

A segunda descoberta é sobre partição, e ela decide duas coisas ao mesmo tempo: **quantas tarefas rodam em paralelo** e **quantos arquivos você deposita no S3**. A mesma escrita, com uma linha de diferença, produz 24 arquivos ou 6.

---

## Passo 1 — Criar o docker-compose.yml

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

```yaml
services:
  floci:
    image: floci/floci:1.5.11
    container_name: floci
    environment:
      - FLOCI_HOSTNAME=floci
      - FLOCI_PORT=4566
      # Modo memoria: os buckets somem junto com o container. E o que se quer
      # num laboratorio -- o notebook recria o bucket a cada execucao.
      - FLOCI_STORAGE_MODE=memory
    ports:
      # 4567 no host, e nao 4566: a porta padrao costuma estar tomada por outro
      # LocalStack/floci -- foi o que aconteceu na validacao, e o `up` inteiro
      # falha com "port is already allocated".
      - "4567:4566"
    networks:
      - spark-net
    healthcheck:
      # A raiz do S3 responde ListAllMyBuckets. Se isso volta 200, o endpoint
      # esta pronto para receber os PUT do Spark.
      test: ["CMD-SHELL", "curl -sf http://localhost:4566/ -o /dev/null"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 5s

networks:
  spark-net:
    name: aula03-spark-network
    driver: bridge
```

**Um serviço só.** Como o Spark roda na sua máquina, não há contêiner de Jupyter nem cluster: o Docker aqui existe apenas para dar um S3 de verdade, com a mesma API da AWS.

**A porta publicada é 4567, mapeada para a 4566 de dentro.** O endpoint do laboratório é `http://localhost:4567`. A 4566 é a porta canônica do LocalStack e costuma já estar ocupada — na validação deste laboratório estava, e o `docker compose up` falhava inteiro com `Bind for 0.0.0.0:4566 failed: port is already allocated`.

## Passo 2 — Subir o S3

```bash
docker compose up -d --wait
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:4567/
```

```
200
```

**Leva 5 s.** O `200` é a resposta de `ListAllMyBuckets` — o S3 está pronto para receber escrita.

## Passo 3 — Criar o ambiente Python

O PySpark 3.5 roda em **Python 3.8 a 3.12**. Não use 3.13 nem 3.14: a instalação até funciona, a execução não.

```bash
py -0        # Windows: lista as versoes instaladas
```

Crie o ambiente **dentro desta pasta**, para o VS Code oferecê-lo como kernel automaticamente:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" ipykernel
```

No Linux ou macOS:

```bash
python3.11 -m venv .venv
./.venv/bin/python -m pip install "pyspark==3.5.3" ipykernel
```

### Registrar o kernel com nome próprio

Este passo parece supérfluo e não é. O VS Code lista os interpretadores pela versão, e se você tiver **outro** Python 3.11 na máquina — o que é comum, já que o `.venv` foi criado a partir de um — a lista mostra duas entradas idênticas, `Python 3.11.15` e `Python 3.11.15`. Escolher a errada dá:

```
Running cells with 'Python 3.11.15' requires the ipykernel package.
```

O erro fala de `ipykernel`, mas o problema é o interpretador: você selecionou o Python base, não o `.venv`.

```powershell
.\.venv\Scripts\python.exe -m ipykernel install --user --name aula03-spark --display-name "Python 3.11 (AULA03 Spark)"
```

```bash
./.venv/bin/python -m ipykernel install --user --name aula03-spark --display-name "Python 3.11 (AULA03 Spark)"
```

Agora existe um kernel com nome inconfundível, e o notebook já vem pedindo por ele — o `aula03-spark.ipynb` traz `"name": "aula03-spark"` nos metadados, então o VS Code o seleciona sozinho assim que ele estiver registrado.

### Java

O Spark é JVM. Confira que existe uma:

```bash
java -version
```

```
java version "1.8.0_461"
```

Serve **JDK 8, 11 ou 17** — validado aqui com o 8. Sem Java o Spark não sobe, e a mensagem não é óbvia.

## Passo 4 — Baixar o que falta para o S3 (e, no Windows, para o Hadoop)

### Os jars do S3

A instalação do PySpark traz o Spark completo, mas **não traz** as bibliotecas que ensinam o Hadoop a falar com S3. Sem elas, `s3a://` não existe como esquema.

```bash
mkdir -p jars && cd jars
M=https://repo1.maven.org/maven2
[ -f hadoop-aws-3.3.4.jar ] || curl -sSLO $M/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
[ -f aws-java-sdk-bundle-1.12.262.jar ] || curl -sSLO $M/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
cd ..
```

São **275 MB**, baixados em ~16 s. Os testes `[ -f ... ] ||` tornam o comando repetível.

**A versão do `hadoop-aws` precisa bater com a do Hadoop embutido no PySpark**, que aqui é 3.3.4:

```bash
ls .venv/Lib/site-packages/pyspark/jars/ | grep hadoop-client-api
```

```
hadoop-client-api-3.3.4.jar
```

Trocar por outra versão dá `NoSuchMethodError` em tempo de execução, não na criação da sessão.

### `winutils.exe` — só no Windows

O Hadoop no Windows precisa de dois binários nativos. **Sem os jars de S3 isso não aparece** — o Spark sobe normalmente. Assim que `hadoop-aws` entra, a classe `Shell` do Hadoop é inicializada e falha:

```
java.lang.RuntimeException: java.io.FileNotFoundException:
HADOOP_HOME and hadoop.home.dir are unset.
```

```bash
mkdir -p hadoop/bin && cd hadoop/bin
R=https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin
[ -f winutils.exe ] || curl -sSLO $R/winutils.exe
[ -f hadoop.dll ]   || curl -sSLO $R/hadoop.dll
cd ../..
```

O notebook aponta `HADOOP_HOME` para essa pasta sozinho. Os binários da série 3.3.5 funcionam com o Hadoop 3.3.4.

> As pastas `jars/`, `hadoop/` e `.venv/` estão no `.gitignore` — são 275 MB de dependências, não código do laboratório.

## Passo 5 — Rodar o notebook no VS Code

1. Abra `aula03-spark.ipynb`;
2. Confirme, no canto superior direito, que o kernel é **Python 3.11 (AULA03 Spark)**;
3. **Run All**.

Se o VS Code não o encontrar, clique em **Select Kernel** ▸ **Jupyter Kernel...** e escolha-o pelo nome. Evite a lista **Python Environments...**: é ali que os dois `Python 3.11.15` aparecem lado a lado, sem indicação de qual é o `.venv`.

A primeira célula confirma o ambiente antes de qualquer outra coisa:

```
python     : 3.11.15 | C:\repo\aula-pratica\AULA03\SPARK\.venv\Scripts\python.exe
pyspark    : 3.5.3
java       : java version "1.8.0_461"
endpoint S3: http://localhost:4567
```

**A execução completa leva ~2,5 minutos** (144 s medidos).

### Por que a seção 0 verifica tudo isso

Cada uma das quatro dependências do ambiente falha de um jeito que não aponta para a causa:

| Falta | Mensagem que você recebe |
| --- | --- |
| kernel errado (Python 3.14, ou sem pyspark) | `ModuleNotFoundError: No module named 'pyspark'` |
| Java | erro de gateway do py4j, sem citar Java |
| `winutils.exe` (Windows) | `HADOOP_HOME and hadoop.home.dir are unset` |
| jars do S3 | `No FileSystem for scheme: s3a` |

A seção 0 checa as quatro e falha com instrução. Ela também exporta `PYSPARK_PYTHON` apontando para o próprio interpretador — sem isso o Spark lança os *workers* com o `python` do `PATH`, que pode ser outra versão, e o sintoma (`Python worker failed to connect back`) não diz nada sobre versão de Python.

---

## O notebook, seção por seção

Tudo abaixo já está no arquivo. Esta parte explica o que observar.

### 1. A sessão

Sete linhas de `spark.hadoop.fs.s3a.*` configuram o S3. Três merecem atenção:

```python
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

Sem isso o cliente monta `http://aula03-lake.localhost:4567` — nome de bucket como subdomínio, do jeito que a AWS faz. Não existe DNS para isso aqui.

```python
.config("spark.sql.shuffle.partitions", "8")
```

O padrão é **200**. Num laboratório de 2 milhões de linhas, isso significa 200 tarefas para processar quase nada cada uma — mais tempo de agendamento do que de trabalho.

```python
.config("spark.python.authenticate.socketTimeout", "120")
```

No Windows o Spark não usa daemon de *workers*: cada tarefa que precisa de Python sobe um processo novo, e o padrão de 15 s para ele se conectar de volta às vezes não basta. Durante a validação, uma execução falhou exatamente assim e a seguinte passou — é intermitente, e este parâmetro é o que a estabiliza.

### 3. Criar um DataFrame no código

```python
pequeno = spark.createDataFrame(linhas, schema)
```

Com schema explícito, e não inferido. Declarar os tipos evita que o Spark leia os dados uma vez só para adivinhar — e evita que ele adivinhe errado, o que numa coluna de código de produto com zeros à esquerda custa caro.

### 5. Partições

A partição é a unidade de paralelismo: uma partição, uma tarefa, um core por vez.

```python
def perfil(rotulo, df):
    tamanhos = df.rdd.glom().map(len).collect()
    print(f"{rotulo:<24} {len(tamanhos):>3} particoes  {tamanhos}")
```

O `glom()` não é firula. `getNumPartitions()` devolve o número planejado, que o otimizador ainda pode mudar; `glom()` materializa o conteúdo de cada partição e mostra o que de fato aconteceu.

```
original                   4 particoes  [500000, 500000, 500000, 500000]
repartition(12)           12 particoes  [166667, 166666, 166666, ...]
coalesce(2)                2 particoes  [1000000, 1000000]
repartition('uf')          5 particoes  [333239, 332628, 666180, 334168, 333785]
repartition(3,'uf')        3 particoes  [665867, 333785, 1000348]
```

As três primeiras linhas são o esperado. As duas últimas são a lição.

**`repartition("uf")` pediu partições por UF e entregou 5, não 6.** São seis valores distribuídos por hash em oito espaços: dois caem no mesmo, e a partição resultante fica com **666 mil linhas contra 333 mil** das outras. Uma tarefa demora o dobro das demais, e o job inteiro espera por ela.

Isso é *skew*, e é a causa mais comum de "o Spark travou em 99%". Repare que ele não veio de dado torto — os dados são perfeitamente uniformes. Veio do hash.

| Função | O que faz | Embaralha? |
| --- | --- | --- |
| `repartition(n)` | redistribui em `n` partições iguais | sim |
| `repartition("col")` | agrupa pelo hash da coluna | sim |
| `coalesce(n)` | junta partições vizinhas, só reduz | **não** |

`coalesce` é mais barato justamente por não embaralhar — e por isso não equilibra nada. Para reduzir antes de gravar, serve. Para corrigir *skew*, não.

### 6. As funções de transformação

| Função | Para quê |
| --- | --- |
| `select` | escolhe colunas — o otimizador usa isso para não ler as outras |
| `filter` / `where` | descarta linhas o mais cedo possível no plano |
| `withColumn` | cria ou substitui uma coluna |
| `when().otherwise()` | condicional, no lugar de `if` |
| `groupBy().agg()` | agrega — **embaralha** |
| `orderBy` | ordena globalmente — **embaralha** |
| `join` | combina — embaralha, a menos que caiba um `broadcast` |
| `Window` | agrega sem colapsar as linhas |

Duas valem comentário.

**`broadcast` no join.** A tabela de clientes tem 50 mil linhas; a de pedidos, 1,8 milhão. `F.broadcast(clientes)` manda a pequena inteira para cada executor e elimina o embaralhamento do lado grande. Vale enquanto a tabela couber com folga na memória — é a otimização de maior retorno em pipelines com tabelas de apoio.

**Janela não é `groupBy`.** O `groupBy` colapsa: 30 linhas viram 1. A janela preserva: cada linha continua existindo e ganha o valor do grupo ao lado.

```
| uf|categoria|     receita|posicao|dif_para_1o|
| BA|papelaria|155737750.80|      1|       0.00|
| BA|   livros|155090978.21|      2| -646772.59|
```

`dif_para_1o` compara cada linha com a primeira da sua UF sem nenhum join.

> **Sobre o `cast("decimal(18,2)")` nas somas.** Sem ele a receita sai como `1.5702142926E8`. Soma de `double` grande vira notação científica na exibição — e, pior, o resultado muda nas últimas casas conforme o número de partições, porque a ordem das adições muda. Para dinheiro, `decimal`.

### 7. UDF

```python
udf_categoria = F.udf(lambda c: c.upper() if c else None, StringType())
```

Uma UDF em Python é opaca para o Catalyst: ele não sabe o que está dentro, então não reordena, não elimina e não empurra o filtro para antes dela. E cada linha atravessa a fronteira JVM ↔ Python.

O notebook mede primeiro **do jeito errado**, de propósito — vale rodar a célula e ver a UDF "ganhar" antes de ler a explicação.

| Medição | Nativa | UDF | Conclusão |
| --- | --- | --- | --- |
| terminando em `.count()` | 193 ms | 185 ms | **falsa** — nada foi calculado |
| terminando em `.agg(F.max(...))` | 411 ms | **5534 ms** | 13,5× |

A regra que fica: **para medir uma transformação, a ação precisa consumir o resultado dela.** `count()` não consome — o Spark descobre isso e joga o cálculo fora.

> **Por que 13,5× e não 3×.** O mesmo notebook rodando dentro de um contêiner Linux mediu **3,2×** para esta UDF. A diferença é o Windows: lá o Spark reaproveita um daemon de *workers* Python, aqui ele sobe um processo por tarefa. A conclusão não muda — a UDF é a parte cara —, mas o número depende do sistema operacional.

Quando não der para evitar a UDF, `pandas_udf` processa em lote via Arrow e recupera boa parte da diferença.

### 8. Gravar no S3

```python
saida.write.mode("overwrite").partitionBy("uf").parquet(f"s3a://{BUCKET}/curated/pedidos")
```

`partitionBy` cria a estrutura de pastas do padrão Hive:

```
curated/pedidos/uf=BA/part-00000-....snappy.parquet
```

E aí aparece o efeito que a seção 5 preparou:

```
24 arquivos parquet  {'uf=BA': 4, 'uf=MG': 4, 'uf=PE': 4, 'uf=RJ': 4, 'uf=RS': 4, 'uf=SP': 4}
 6 arquivos parquet  {'uf=BA': 1, 'uf=MG': 1, 'uf=PE': 1, 'uf=RJ': 1, 'uf=RS': 1, 'uf=SP': 1}
```

Quatro partições em memória, cada uma com linhas de todas as UFs, escrevendo em seis pastas: **4 × 6 = 24 arquivos**. Com `repartition("uf")` antes da escrita, cada UF vira uma partição só, e sai um arquivo por pasta.

Em object storage isso importa mais do que parece: cada arquivo é ao menos uma requisição HTTP na leitura. É o "problema dos arquivos pequenos", e ele nasce exatamente aqui — do número de partições em memória multiplicado pelo número de valores da coluna de particionamento.

### 9. Ler de volta

```
leitura completa ............. 920 ms -> 1840553 linhas
leitura com filtro uf='SP' ... 293 ms -> 305759 linhas
```

O filtro por `uf` não lê as outras UFs: o caminho já diz o que tem dentro. Isso é *partition pruning*, e é o motivo de escolher a coluna de particionamento pelos **filtros que a aplicação vai fazer** — não pela que parece mais organizada.

Particionar por uma coluna de alta cardinalidade (como `pedido_id`) criaria milhões de pastas com um arquivo cada. O critério é: poucos valores distintos, e presentes no `WHERE`.

Por fim:

```
colunas na volta: ['pedido_id', 'categoria', 'mes', 'quantidade', 'valor', 'total', 'uf']
```

`uf` aparece **por último**, embora estivesse no meio na escrita. Ela não está gravada dentro dos arquivos — o Spark a reconstrói a partir do nome da pasta. A coluna de particionamento deixa de ser dado e vira metadado do caminho.

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir o floci (`up -d --wait`) | 5 s |
| Passo 3 — criar o venv e instalar o PySpark | ~90 s |
| Passo 4 — baixar os jars (275 MB, uma vez) | 16 s |
| Notebook completo, de ponta a ponta | **144 s** |
| — escrita `partitionBy` no S3 | 10,3 s |
| — leitura completa de volta | 0,9 s |
| — leitura com *partition pruning* | 0,3 s |
| **Total** | **~5 min** |

Medido em Windows 10, Python 3.11.15, Java 8, `local[4]`. O notebook foi executado de ponta a ponta com `jupyter nbconvert --execute` duas vezes seguidas: **144 s e 135 s**, ambas com saída idêntica.

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| `requires the ipykernel package` | kernel é outro Python 3.11, não o `.venv` | registrar o kernel nomeado (Passo 3) e selecioná-lo |
| `ModuleNotFoundError: No module named 'pyspark'` | mesma causa: interpretador errado | idem — confira o caminho na saída da seção 0 |
| A seção 0 mostra Python 3.13 ou 3.14 | PySpark 3.5 não suporta | recriar o venv com 3.11 ou 3.12 |
| `Python worker failed to connect back` | processo do worker demorou a conectar | `spark.python.authenticate.socketTimeout`; se persistir, rode a célula de novo |
| `HADOOP_HOME and hadoop.home.dir are unset` | falta `winutils.exe` (só Windows) | Passo 4 |
| `No FileSystem for scheme: s3a` | jars do Passo 4 não baixados | Passo 4, e reiniciar o kernel |
| `NoSuchMethodError` ao gravar | `hadoop-aws` de versão diferente | usar 3.3.4, igual ao `hadoop-client-api` |
| `Bind for 0.0.0.0:4566 failed` | outro LocalStack/floci na porta padrão | este compose usa 4567 no host |
| Erro de host desconhecido no bucket | falta `path.style.access=true` | está na seção 1 |
| `409 Conflict` ao criar o bucket | o bucket já existe | tratado na função `criar_bucket` |
| UDF parece mais rápida que a nativa | `count()` fez o otimizador apagar a projeção | agregar a coluna calculada |
| Uma tarefa demora o dobro das outras | *skew* de hash no `repartition("col")` | conferir com `glom()`; `repartition(n)` se não precisar agrupar |
| Milhares de arquivos pequenos no S3 | partições em memória × valores da coluna | `repartition("col")` antes do `write` |
| Receita em notação científica | soma de `double` | `cast("decimal(18,2)")` |

---

## O que levar disso para o trabalho

**Medir Spark exige saber o que o otimizador fez.** A armadilha do `count()` não é curiosidade acadêmica: é a forma mais comum de "provar" que uma otimização funcionou quando ela nunca rodou. Antes de confiar num número, confirme que a ação consome o resultado — e, na dúvida, `df.explain()` mostra o plano que vai executar de fato.

**A partição é a única alavanca que aparece nos dois lados.** Ela decide o paralelismo durante o processamento e o número de arquivos depois dele. Duas perguntas resolvem a maior parte das decisões:

1. *Quantas tarefas quero em paralelo?* — governa `repartition(n)`.
2. *Por qual coluna a aplicação vai filtrar?* — governa `partitionBy("col")`.

São perguntas diferentes, e responder uma com a outra é o que gera tanto o *skew* quanto o excesso de arquivos.

**Funções nativas não são preferência de estilo.** Elas são as únicas que o Catalyst enxerga. E o custo da alternativa depende de onde você roda: 3,2× num Linux com daemon de workers, 13,5× no Windows sem ele. Antes de escrever uma UDF, vale procurar a nativa equivalente; o módulo `pyspark.sql.functions` cobre muito mais do que parece.

**E `s3a://` é um sistema de arquivos emulado.** Renomear pasta no S3 é copiar e apagar, listar é uma chamada HTTP, e não existe escrita atômica. É por isso que o número de arquivos importa tanto, e por isso que formatos como Delta e Iceberg existem. Este laboratório grava Parquet puro de propósito, para que o problema apareça antes da solução.

---

## Encerrar

```bash
docker compose down -v
```

O `.venv`, os `jars/` e o `hadoop/` ficam no disco — não custam nada parados, e evitam repetir os Passos 3 e 4 na próxima aula.
