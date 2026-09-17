# Laboratório 10 — ETL com PySpark lendo e gravando no S3

Terceira variação do mesmo ETL: depois do disco local (AULA03) e do Azure Blob Storage ([`PYSPARK-AZURE-BLOB`](../PYSPARK-AZURE-BLOB/README.md)), aqui a entrada e a saída vivem num bucket S3. O dado é consumo de energia elétrica por setor (residencial, comercial, industrial, rural) num trimestre (jul–set/2024), nas mesmas dez cidades de Santa Catarina dos outros laboratórios.

Não é preciso ter conta na AWS: o laboratório roda contra o **[floci](https://floci.io/aws/)**, um emulador de AWS em Docker. Trocar para uma conta real depois é variável de ambiente — nenhum script muda.

**Este laboratório é um exercício.** `00_conectar_s3.py` vem pronto, mas `01_etl_energia_sc.py` tem quatro funções de Transform com lacunas para você preencher — enunciados, resposta esperada e gabarito em [`EXERCICIOS_AWS.md`](EXERCICIOS_AWS.md).

## Sumário

- [Antes do código: o vocabulário mínimo](#antes-do-código-o-vocabulário-mínimo)
- [O que você vai descobrir](#o-que-você-vai-descobrir)
- [Instalação](#instalação)
- [Passo a passo para rodar](#passo-a-passo-para-rodar)
- [Script 00 — conectar](#script-00--conectar-ao-s3)
- [Script 01 — o exercício de ETL](#script-01--o-exercício-de-etl)
- [Glossário](#glossário)
- [Se algo der errado](#se-algo-der-errado)
- [O que levar disso para o trabalho](#o-que-levar-disso-para-o-trabalho)

---

## Antes do código: o vocabulário mínimo

| Termo | O que é, sem rodeio |
| --- | --- |
| **S3** | o serviço de armazenamento de objetos da AWS — o equivalente ao Blob Storage do Azure. Guarda arquivos ("objetos"), não linhas de tabela. |
| **Bucket** | o "container de primeiro nível" de uma conta S3. Todo objeto mora dentro de um bucket. Neste laboratório, o bucket é `energia-sc`. |
| **Objeto / chave (key)** | um arquivo dentro do bucket, identificado por uma string. Não existe pasta de verdade — `processado/resumo_regiao/part-00000.parquet` é **uma chave com barras**, não um caminho de diretório. |
| **`endpoint_url`** | o parâmetro do boto3 que diz para onde mandar as chamadas REST. Contra a AWS real, você não passa nada (o SDK resolve sozinho); contra um emulador, você aponta explicitamente. |
| **floci** | um emulador de AWS que implementa a mesma API REST do S3 real, rodando na sua máquina, com credenciais fixas (`test`/`test`) que não precisam ser validadas. O código que fala com ele é *o mesmo* código que falaria com a AWS de verdade. |
| **boto3** | o SDK oficial da AWS para Python — o equivalente ao `azure-storage-blob` do laboratório anterior. |

### Por que baixar em vez de ler direto

Existe um jeito de fazer `spark.read.csv("s3a://bucket/arquivo.csv")` — um conector de sistema de arquivos (`hadoop-aws`) que registra o protocolo `s3a://` dentro do próprio Spark, e cada executor lê e grava direto no S3.

Este laboratório **não** usa esse caminho, pelo mesmo motivo do laboratório do Blob:

1. **`local[4]` não tem cluster.** O conector existe para que **centenas de executores distribuídos** leiam pedaços do mesmo objeto remoto em paralelo. Com um único processo local, isso não compra nada.
2. **Menos uma peça frágil.** `hadoop-aws` depende de `aws-java-sdk-bundle` batendo com a versão do Hadoop embutida no seu PySpark — outro par de JARs, outro descompasso de versão possível, outro erro de classe Java que não menciona S3 em lugar nenhum.

Por isso o padrão aqui é **baixar do S3 com o boto3, processar local com o Spark, subir o resultado de volta com o mesmo boto3** — a mesma arquitetura do laboratório do Blob, com o SDK trocado.

---

## O que você vai descobrir

Todos os números abaixo saíram de uma execução real do `01_etl_energia_sc.py` já resolvido.

### 1. Uma regra de validação pode existir e nunca disparar

Das cinco condições da regra de validação, uma — `unidades_consumidoras > 0` — rejeita **zero** linhas neste dado. Isso não é a regra estar errada nem sobrando: é o gerador de dado sintético nunca produzir esse tipo específico de sujeira. Em produção, uma regra de validação com contagem zero por meses é normal — ela existe para o dia em que um sistema upstream mudar de comportamento, não para o dado de hoje.

### 2. A mesma armadilha do `NULL`, na terceira nuvem

O mecanismo `F.coalesce(regra, F.lit(False))` aparece pela terceira vez consecutiva (AULA03 local, Blob, agora S3) — porque a causa não é o armazenamento, é o SQL: qualquer comparação com `NULL` retorna `NULL`, nunca `False`, e um `filter` só mantém o que é `True`. Trocar Parquet local por Blob ou por S3 não muda uma linha dessa lógica — é por isso que ela é ensinada uma vez e reconhecida três vezes.

### 3. Trocar de nuvem é uma variável de ambiente diferente da do Azure

No laboratório do Blob, a variável de troca era `AZURE_STORAGE_CONNECTION_STRING`, e defini-la já bastava. Aqui, `AWS_ENDPOINT_URL=""` (**vazia, não ausente**) é o que desliga o floci — porque contra a AWS real você normalmente **não define** `endpoint_url` nenhum, deixa o boto3 resolver sozinho pela região. Duas nuvens, duas convenções de "o que significa usar o serviço de verdade" — vale ler a documentação de cada SDK antes de assumir que "limpar a variável" e "não definir a variável" são a mesma coisa.

### Números da execução

```
lidas .............: 3693
apos dropDuplicates: 3680  (-13 duplicatas)
aprovadas .........: 3538
rejeitadas ........: 142
conciliacao .......: OK

setor invalido                                44
municipio invalida ou ausente                 35
tarifa fora da faixa (0.2 a 2.0)              35
consumo negativo                              28
unidades consumidoras invalidas                0
```

Custo total do trimestre por região (R$, decrescente):

```
Norte Catarinense        15.698.326,31
Grande Florianopolis     15.259.130,16
Vale do Itajai           14.393.176,59
Oeste Catarinense         4.362.617,22
Sul Catarinense           4.354.908,94
Serrana                   3.174.153,25
```

---

## Instalação

Guia completo, com o `docker-compose.yml` do floci, no [SETUP.md](SETUP.md). Resumo:

```bash
docker compose up -d --wait                                   # floci
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" boto3
# Windows: baixar hadoop/bin/winutils.exe (Passo 4 do SETUP.md)
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Se você já rodou a AULA03 ou o laboratório do Blob nesta máquina, o Python, o Java e o `winutils.exe` já estão prontos — só falta o floci e o `boto3`.

---

## Passo a passo para rodar

```powershell
.\.venv\Scripts\python.exe 00_conectar_s3.py
.\.venv\Scripts\python.exe 01_etl_energia_sc.py
```

O `00` roda de ponta a ponta sem alterações. O `01`, no estado em que vem no clone, para no primeiro `# TODO` com um erro do tipo `AttributeError: 'NoneType' object has no attribute ...` — é o exercício começando, não um script quebrado. Complete as quatro funções seguindo o [`EXERCICIOS_AWS.md`](EXERCICIOS_AWS.md) e rode de novo a cada mudança.

### O que há em cada arquivo

| Arquivo | Função |
| --- | --- |
| [`comum.py`](comum.py) | sessão Spark local + cliente S3 (credenciais, endpoint, bucket, upload/download) |
| [`00_conectar_s3.py`](00_conectar_s3.py) | conectar, subir, listar, baixar e apagar um objeto — sem Spark |
| [`01_etl_energia_sc.py`](01_etl_energia_sc.py) | **o exercício**: Fonte, Extract, Load e Verificação prontos; quatro funções de Transform com `# TODO` para você completar |
| [`EXERCICIOS_AWS.md`](EXERCICIOS_AWS.md) | os quatro enunciados, a resposta esperada de cada um e o gabarito comentado |
| [`verificar_ambiente.py`](verificar_ambiente.py) | confere Python, Java, PySpark, `winutils`, boto3 e o floci respondendo |

---

## Script 00 — conectar ao S3

Cinco passos, cada um uma operação isolada do boto3:

```python
s3 = obter_bucket()                                    # 1. conectar (cria o bucket se preciso)
s3.put_object(Bucket=BUCKET, Key=..., Body=...)         # 2. subir
s3.list_objects_v2(Bucket=BUCKET, Prefix=...)           # 3. listar
s3.get_object(Bucket=BUCKET, Key=...)["Body"].read()    # 4. baixar
s3.delete_object(Bucket=BUCKET, Key=...)                # 5. apagar
```

Se este script rodar sem erro, o `01_etl_energia_sc.py` também vai — as duas únicas operações de rede que ele faz (`upload_file`/`download_file`) já foram provadas aqui.

## Script 01 — o exercício de ETL

### A entrada

Um trimestre inteiro (92 dias) de leituras diárias de consumo, cruzando dez cidades com quatro setores — `leitura_id`, `data`, `municipio`, `setor`, `consumo_kwh`, `unidades_consumidoras`, `tarifa_rs_kwh`. Gerada com semente fixa (`random.Random(19)`), com ~5% de sujeira: município ausente ou em minúsculo, setor inválido, consumo negativo, tarifa fora de faixa, e algumas duplicatas.

### Extract (pronto)

```python
baixar_arquivo(s3, "bruto/energia_sc.csv", DADOS / "_baixado_energia.csv")
bruto = spark.read.schema(SCHEMA_ENERGIA).csv(str(DADOS / "_baixado_energia.csv"))
```

O download acontece **antes** do `spark.read` — a mesma separação do laboratório do Blob: uma operação de rede (boto3), uma de disco (Spark).

### Transform (o exercício)

Normalizar (`upper`/`lower`/`trim` + `dropDuplicates`), validar com `F.coalesce(regra, F.lit(False))`, montar o `motivo` da rejeição com uma cadeia de `F.when`, enriquecer (`custo_total`, `faixa_consumo`) e resumir por região. É a parte que não muda seja qual for o armazenamento por trás — fica para você escrever.

### Load (pronto)

Grava Parquet particionado por `municipio` **localmente**, depois sobe cada arquivo gerado para o S3 com `subir_pasta()`.

### Verificação (pronta)

Baixa de volta um dos arquivos Parquet do resumo — não para reprocessar nada, só para provar que o que subiu é o que desce.

---

## Glossário

| Termo | Explicação |
| --- | --- |
| `head_bucket` / `create_bucket` | como o `comum.py` verifica se o bucket existe antes de criar — o equivalente ao `container.exists()` do laboratório do Blob. |
| `LocationConstraint` | parâmetro que diz em qual região criar o bucket — **não pode ser enviado** quando a região é `us-east-1`, uma das inconsistências históricas da API do S3. |
| `list_objects_v2` | lista até 1000 chaves por chamada; mais que isso exige paginação com `ContinuationToken`, sem uso neste laboratório. |
| `ClientError` | exceção genérica do `botocore` para qualquer erro HTTP da AWS — o código específico (`NoSuchKey`, `404`, etc.) vem em `erro.response["Error"]["Code"]`. |
| `upload_file` / `download_file` | métodos de alto nível do boto3 que cuidam de multipart automaticamente — preferíveis a `put_object`/`get_object` para arquivos grandes. |

Para o vocabulário do Spark (DataFrame, transformação × ação, partição, `Window`), veja o [glossário da AULA03](../PYSPARK-BASICO/README.md#glossário) — é o mesmo motor.

---

## Se algo der errado

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `EndpointConnectionError` | o floci não está rodando | `docker compose up -d --wait`, confira com `docker ps` |
| `ModuleNotFoundError: No module named 'boto3'` | pacote não instalado neste interpretador | `.\.venv\Scripts\python.exe -m pip install boto3` |
| `InvalidLocationConstraint` ao criar o bucket | `AWS_DEFAULT_REGION` mudada sem revisar `obter_bucket()` | deixe a região padrão (`us-east-1`) ou revise a lógica de `CreateBucketConfiguration` |
| ETL roda mas `saida/` fica vazia | a pasta `dados/` tinha um CSV antigo de uma execução anterior com schema diferente | apague `dados/` e `saida/` e rode de novo |
| erro de gateway do py4j, `HADOOP_HOME` ausente, `UnsupportedClassVersionError` | mesmas causas da AULA03 (Java, `winutils`, versão do PySpark) | veja a tabela do [README da AULA03](../PYSPARK-BASICO/README.md#se-algo-der-errado) |
| `AWS_ENDPOINT_URL` definida mas o script ainda usa o floci | variável exportada no terminal errado, ou definida como string não vazia por engano | confira com `echo $AWS_ENDPOINT_URL` (`echo $env:AWS_ENDPOINT_URL` no PowerShell) antes de rodar |

---

## O que levar disso para o trabalho

Este é o terceiro laboratório com a mesma estrutura de Extract/Transform/Load e o terceiro lugar diferente onde os dados moram — disco local, Blob, S3. A parte que **nunca mudou** entre os três foi a validação: `F.coalesce(regra, F.lit(False))` mais uma cadeia de `F.when` para o motivo. É essa estabilidade que justifica separar a lógica de negócio (o que é um dado válido) da infraestrutura de E/S (de onde ele vem e para onde vai) — quando as duas se misturam num único bloco de código, trocar de nuvem vira reescrever regra de negócio por acidente.

A diferença de convenção entre `AZURE_STORAGE_CONNECTION_STRING` (definir para trocar) e `AWS_ENDPOINT_URL=""` (esvaziar para trocar) também é uma lição por si só: **cada provedor de nuvem tem sua própria ideia do que significa "modo padrão"**, e um script que funciona nos dois não é o que usa a mesma variável — é o que isola a decisão numa função (`obter_bucket()`, `obter_container_client()`) pequena o bastante para reler antes de confiar.
