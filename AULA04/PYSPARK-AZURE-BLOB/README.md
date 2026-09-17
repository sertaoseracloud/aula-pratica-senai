# Laboratório 9 — ETL com PySpark lendo e gravando no Azure Blob Storage

Continuação direta da AULA03: o mesmo ETL — Extract, Transform, Load, com validação e motivo de rejeição — só que agora a entrada e a saída vivem num Blob Storage, não no seu disco. O dado desta vez é temperatura de um trimestre (jul–set/2024) em dez cidades de Santa Catarina, simulada com semente fixa.

Não é preciso ter conta no Azure: o laboratório roda contra o **[floci-az](https://floci.io/az/)**, um emulador de Azure em Docker (o mesmo Blob Storage, entre outros serviços). Trocar para uma conta real depois é uma variável de ambiente — nenhum script muda.

**Este laboratório é um exercício.** `00_conectar_blob.py` vem pronto, mas `01_etl_temperaturas_sc.py` tem quatro funções de Transform com lacunas para você preencher — enunciados, resposta esperada e gabarito em [`EXERCICIOS_AZURE.md`](EXERCICIOS_AZURE.md).

## Sumário

- [Antes do código: o vocabulário mínimo](#antes-do-código-o-vocabulário-mínimo)
- [O que você vai descobrir](#o-que-você-vai-descobrir)
- [Instalação](#instalação)
- [Passo a passo para rodar](#passo-a-passo-para-rodar)
- [Script 00 — conectar](#script-00--conectar-ao-blob)
- [Script 01 — o exercício de ETL](#script-01--o-exercício-de-etl)
- [Glossário](#glossário)
- [Se algo der errado](#se-algo-der-errado)
- [O que levar disso para o trabalho](#o-que-levar-disso-para-o-trabalho)

---

## Antes do código: o vocabulário mínimo

| Termo | O que é, sem rodeio |
| --- | --- |
| **Blob Storage** | o serviço de armazenamento de objetos do Azure — o equivalente ao S3 da AWS. Guarda arquivos ("blobs"), não linhas de tabela. |
| **Container** | a "pasta de primeiro nível" dentro de uma conta de Storage. Todo blob mora dentro de um container. Neste laboratório, o container é `temperaturas-sc`. |
| **Blob** | um arquivo dentro do container. Não existe pasta de verdade — `processado/resumo_regiao/part-00000.parquet` é **um nome de blob com barras**, não um caminho de diretório. |
| **Connection string** | a credencial que diz ao SDK onde está a conta e como se autenticar. Contém conta, chave e endereço num único texto. |
| **floci-az** | um emulador de Azure que implementa a mesma API REST do Blob Storage real, rodando na sua máquina, com a mesma conta de desenvolvimento padrão que o Azurite usa. O código que fala com ele é *o mesmo* código que falaria com o Azure de verdade. |
| **SDK vs. conector de sistema de arquivos** | duas formas diferentes de acessar o Blob a partir do Spark. Este laboratório usa a primeira — veja por quê logo abaixo. |

### Por que baixar em vez de ler direto

Existe um jeito de fazer `spark.read.csv("wasb://container@conta.blob.core.windows.net/arquivo.csv")` — um conector de sistema de arquivos (`hadoop-azure`) que registra o protocolo `wasb://`/`abfss://` dentro do próprio Spark, e cada executor lê e grava direto no Blob.

Este laboratório **não** usa esse caminho, de propósito, por dois motivos:

1. **`local[4]` não tem cluster.** O conector de sistema de arquivos existe para permitir que **centenas de executores distribuídos** leiam pedaços do mesmo arquivo remoto em paralelo. Com um único processo local, isso não compra nada — o arquivo inteiro ia ser baixado de um jeito ou de outro.
2. **Menos uma peça frágil.** O conector depende de duas versões de JAR (`hadoop-azure` e `azure-storage`) baterem exatamente com a versão do Hadoop embutida no seu PySpark. Um descompasso de versão produz um erro de classe Java que não menciona Azure em lugar nenhum — o mesmo tipo de sintoma "genérico" que o `verificar_ambiente.py` da AULA03 existe para evitar.

Por isso o padrão aqui é **baixar do Blob com o SDK do Python, processar local com o Spark, subir o resultado de volta com o SDK**. É também o padrão mais comum na prática para jobs de porte pequeno/médio sem cluster dedicado — muita gente que "usa Spark com Azure" está fazendo exatamente isto, não o `wasb://` direto.

---

## O que você vai descobrir

Todos os números abaixo saíram de uma execução real do `01_etl_temperaturas_sc.py`.

### 1. A mesma armadilha do `NULL` aparece de novo, com outra cara

Na AULA03, uma condição de validação que vira `NULL` fazia 505 linhas evaporarem dos dois lados (aprovadas e rejeitadas) sem erro nenhum. Aqui a proteção é a mesma — `F.coalesce(regra, F.lit(False))` — mas o gatilho é diferente: sensores de temperatura têm campo `municipio` vazio, e `F.col("municipio").isin(...)` sobre uma string vazia não é `NULL` (é `False`, o `isin` sabe lidar com string vazia). O `NULL` de verdade viria de um valor genuinamente ausente no CSV — o mesmo mecanismo, esperando o próximo formato de sujeira que ninguém previu.

### 2. Duas regras de validação podem disputar a mesma linha

Das 11 leituras rejeitadas por `min > max`, 7 também violam a faixa física (-15 a 50 °C) — são casos em que o sensor grava `temperatura_max` como `-25 °C` mantendo o `temperatura_min` original (~15–25 °C). Essa linha quebra **duas** regras ao mesmo tempo, e a ordem do `F.when(...)` decide qual motivo fica registrado — a primeira condição que bater "ganha" (aqui, `min > max` vem antes). Isso não é um bug: é a mesma lógica de `CASE WHEN` do SQL, mas é fácil esquecer que ela existe até o dado real expor o caso.

### 3. Trocar de nuvem local para nuvem real é uma variável de ambiente, não uma reescrita

Nenhuma linha dos scripts menciona "floci-az" fora do valor padrão de uma constante. `AZURE_STORAGE_CONNECTION_STRING` no ambiente muda tudo — inclusive o protocolo (`http` local vira `https` real) e o host. É a mesma separação que a AULA03 não precisou fazer (não há "S3 de mentira" na versão local dela) — e é o motivo pelo qual golpear a connection string direto no código é considerado um erro grave em qualquer revisão: ele acopla o pipeline a um único ambiente.

### Números da execução

```
lidas .............: 924
apos dropDuplicates: 920  (-4 duplicatas)
aprovadas .........: 886
rejeitadas ........: 34
conciliacao .......: OK

temperatura_min maior que temperatura_max     11
municipio invalida ou ausente                  8
umidade fora da faixa (0 a 100)                8
temperatura fora da faixa fisica (-15 a 50)    7
```

Média trimestral por região (°C, decrescente):

```
Grande Florianopolis     19.72
Vale do Itajai           19.49
Norte Catarinense        19.01
Sul Catarinense          18.21
Oeste Catarinense        17.66
Serrana                  14.66
```

A Serrana (Lages) sai **5 graus** abaixo da região mais quente — plausível para quem conhece o inverno na serra catarinense, e uma checagem rápida de que o dado sintético não saiu absurdo.

---

## Instalação

Guia completo, com o `docker-compose.yml` do floci-az, no [SETUP.md](SETUP.md). Resumo:

```bash
docker compose up -d --wait                                   # floci-az
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" azure-storage-blob
# Windows: baixar hadoop/bin/winutils.exe (Passo 5 do SETUP.md)
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Se você já rodou a AULA03 nesta máquina, o Python, o Java e o `winutils.exe` já estão prontos — só falta o floci-az e o `azure-storage-blob`.

---

## Passo a passo para rodar

```powershell
.\.venv\Scripts\python.exe 00_conectar_blob.py
.\.venv\Scripts\python.exe 01_etl_temperaturas_sc.py
```

O `00` roda de ponta a ponta sem alterações. O `01`, no estado em que vem no clone, para no primeiro `# TODO` com um erro do tipo `AttributeError: 'NoneType' object has no attribute ...` — é o exercício começando, não um script quebrado. Complete as quatro funções seguindo o [`EXERCICIOS_AZURE.md`](EXERCICIOS_AZURE.md) e rode de novo a cada mudança.

### O que há em cada arquivo

| Arquivo | Função |
| --- | --- |
| [`comum.py`](comum.py) | sessão Spark local + cliente do Blob (connection string, container, upload/download) |
| [`00_conectar_blob.py`](00_conectar_blob.py) | conectar, subir, listar, baixar e apagar um blob — sem Spark |
| [`01_etl_temperaturas_sc.py`](01_etl_temperaturas_sc.py) | **o exercício**: Fonte, Extract, Load e Verificação prontos; quatro funções de Transform com `# TODO` para você completar |
| [`EXERCICIOS_AZURE.md`](EXERCICIOS_AZURE.md) | os quatro enunciados, a resposta esperada de cada um e o gabarito comentado |
| [`verificar_ambiente.py`](verificar_ambiente.py) | confere Python, Java, PySpark, `winutils`, SDK do Azure e o floci-az respondendo |

---

## Script 00 — conectar ao Blob

Cinco passos, cada um uma operação isolada do SDK:

```python
container = obter_container_client()      # 1. conectar (cria o container se preciso)
container.upload_blob(...)                # 2. subir
container.list_blobs(...)                 # 3. listar
container.get_blob_client(...).download_blob().readall()  # 4. baixar
container.delete_blob(...)                # 5. apagar
```

Se este script rodar sem erro, o `01_etl_temperaturas_sc.py` também vai — as duas únicas operações de rede que ele faz (`upload_blob`/`download_blob`) já foram provadas aqui.

## Script 01 — o exercício de ETL

`01_etl_temperaturas_sc.py` **não vem pronto**: é o exercício. Fonte, Extract, Load e Verificação já estão implementados; quatro funções de Transform (`normalizar`, `validar`, `enriquecer`, `resumir_por_regiao`) têm um `# TODO Exercicio N` no lugar do código e devolvem `None` até você completá-las. Os quatro enunciados, com a resposta esperada e o gabarito comentado, estão em [`EXERCICIOS_AZURE.md`](EXERCICIOS_AZURE.md).

### A entrada

Um trimestre inteiro (1º de julho a 30 de setembro de 2024 — 92 dias) de leituras diárias em dez cidades: `estacao_id`, `data`, `municipio`, `temperatura_min`, `temperatura_max`, `temperatura_media`, `umidade_pct`. Gerada uma vez com semente fixa (`random.Random(7)`), com ~5% de sujeira plausível para um sensor de campo: município ausente ou em minúsculo, mínimo e máximo trocados, leitura fora da faixa física, umidade fora de 0–100%, e algumas duplicatas.

### Extract (pronto)

```python
baixar_arquivo(container, "bruto/temperaturas_sc.csv", DADOS / "_baixado_temperaturas.csv")
bruto = spark.read.schema(SCHEMA_TEMPERATURAS).csv(str(DADOS / "_baixado_temperaturas.csv"))
```

O download acontece **antes** do `spark.read` — são duas operações separadas, uma de rede (SDK) e uma de disco (Spark), não uma coisa só.

### Transform (o exercício)

Mesma estrutura da AULA03: normalizar (`upper`/`trim` + `dropDuplicates`), validar com `F.coalesce(regra, F.lit(False))`, montar o `motivo` da rejeição com uma cadeia de `F.when`, e só então enriquecer (`amplitude_termica`, `faixa_dia`) e juntar com a tabela de região. É exatamente essa parte — a que não muda seja qual for a nuvem por trás — que fica para você escrever.

### Load (pronto)

Grava Parquet particionado por `municipio` **localmente**, depois sobe cada arquivo gerado para o Blob com `subir_pasta()` — que percorre a pasta recursivamente e transforma cada caminho relativo num nome de blob com barras. `_SUCCESS` e os `.crc` do Hadoop ficam de fora: são metadado do driver local, não fariam sentido reaparecendo no Blob.

### Verificação (pronta)

Baixa de volta um dos arquivos Parquet do resumo — não para reprocessar nada, só para provar que o que subiu é o que desce.

---

## Glossário

| Termo | Explicação |
| --- | --- |
| `ContainerClient` | objeto do SDK que representa um container já aberto; é nele que `upload_blob`, `download_blob` e `list_blobs` vivem. |
| `BlobServiceClient` | o "nível conta" do SDK — de onde se pega um `ContainerClient`. |
| `name_starts_with` | filtro de prefixo em `list_blobs`, resolvido no servidor — não baixa o que não interessa. |
| `overwrite=True` | em `upload_blob`, permite subir por cima de um blob que já existe; sem isso, a segunda execução falha com `ResourceExistsError`. |
| `ResourceNotFoundError` / `ResourceExistsError` | exceções de `azure.core.exceptions` — o Azure fala REST por baixo, e o SDK traduz os códigos HTTP (404, 409) para essas classes. |

Para o vocabulário do Spark (DataFrame, transformação × ação, partição, `Window`), veja o [glossário da AULA03](../PYSPARK-BASICO/README.md#glossário) — é o mesmo motor.

---

## Se algo der errado

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `ServiceRequestError: Connection refused` | o floci-az não está rodando | `docker compose up -d --wait`, confira com `docker ps` |
| `ModuleNotFoundError: No module named 'azure'` | SDK não instalado neste interpretador | `.\.venv\Scripts\python.exe -m pip install azure-storage-blob` |
| `ResourceExistsError` ao criar o container | duas execuções simultâneas, ou uma checagem removida | o `comum.py` já testa `container.exists()` antes de criar — não remova essa checagem |
| ETL roda mas `saida/` fica vazia | a pasta `dados/` tinha um CSV antigo de uma execução anterior com schema diferente | apague `dados/` e `saida/` e rode de novo |
| erro de gateway do py4j, `HADOOP_HOME` ausente, `UnsupportedClassVersionError` | mesmas causas da AULA03 (Java, `winutils`, versão do PySpark) | veja a tabela do [README da AULA03](../PYSPARK-BASICO/README.md#se-algo-der-errado) |
| `AZURE_STORAGE_CONNECTION_STRING` definida mas o script ainda usa o floci-az | variável exportada no terminal errado, ou com erro de digitação | confira com `echo $AZURE_STORAGE_CONNECTION_STRING` (`echo $env:AZURE_STORAGE_CONNECTION_STRING` no PowerShell) antes de rodar |

---

## O que levar disso para o trabalho

A AULA03 mostrou que o Spark aceita uma medição que não mediu nada, e que um `NULL` numa condição de validação apaga linha sem avisar. Este laboratório acrescenta uma terceira lição, específica de quem processa dado que mora na nuvem: **o SDK que fala com o armazenamento e o motor que processa o dado não precisam ser a mesma peça** — e forçá-los a ser (com um conector de sistema de arquivos configurado errado) costuma custar mais em depuração do que economiza em linhas de código.

A separação também paga em portabilidade: como todo acesso ao Blob passa por quatro funções em `comum.py` (`obter_container_client`, `subir_arquivo`, `baixar_arquivo`, `subir_pasta`), trocar o floci-az por Azure real — ou por outro provedor com uma API compatível — é mudar uma variável de ambiente, não caçar `wasb://` espalhado pelo código.
