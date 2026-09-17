# Laboratório 12 — arquitetura medalhão com Azure Blob Storage

Junta as duas ideias das aulas anteriores: a arquitetura medalhão do [`PYSPARK-MEDALHAO`](../PYSPARK-MEDALHAO/README.md) (três jobs independentes, um por camada) e o Blob Storage do [ETL da AULA04](../../AULA04/PYSPARK-AZURE-BLOB/README.md). Aqui, cada camada — Bronze, Silver, Gold — não é só uma pasta no seu disco: é um prefixo no Blob (emulado localmente pelo floci-az). O dado é pluviometria (chuva e umidade) de um trimestre nas mesmas dez cidades de SC dos outros laboratórios.

**Este laboratório é um exercício.** `00_conectar_blob.py` vem pronto; os três jobs têm cinco funções incompletas ao todo — enunciados, resposta esperada e gabarito em [`EXERCICIOS_CHUVA.md`](EXERCICIOS_CHUVA.md).

## Sumário

- [O que muda em relação aos dois laboratórios anteriores](#o-que-muda-em-relação-aos-dois-laboratórios-anteriores)
- [O que você vai descobrir](#o-que-você-vai-descobrir)
- [Instalação](#instalação)
- [Passo a passo para rodar](#passo-a-passo-para-rodar)
- [Os três jobs](#os-três-jobs)
- [Se algo der errado](#se-algo-der-errado)
- [O que levar disso para o trabalho](#o-que-levar-disso-para-o-trabalho)

---

## O que muda em relação aos dois laboratórios anteriores

| | `PYSPARK-MEDALHAO` (local) | `PYSPARK-AZURE-BLOB` da AULA04 (ETL) | Este laboratório |
| --- | --- | --- | --- |
| Onde a camada vive | pasta no disco | não se aplica (um script só) | prefixo no Blob |
| Quantos jobs | 3, um por camada | 1, com 4 funções | 3, um por camada |
| Quem lê a camada anterior | `spark.read.parquet(pasta_local)` | não se aplica | `baixar_pasta()` do Blob, depois `spark.read.parquet(copia_local)` |
| Como um job confere que a entrada existe | `Path.exists()` | `container.get_blob_client(...).exists()` | `prefixo_existe()` — lista o prefixo e olha se veio algo |

A combinação das duas ideias trouxe uma função nova que nenhum dos dois laboratórios anteriores precisava: **`baixar_pasta()`**, que baixa não um arquivo, mas todos os blobs sob um prefixo — porque uma camada Parquet é sempre uma pasta com vários arquivos `part-....parquet`, não um objeto só. O `subir_pasta()` já existia (o ETL da AULA04 subia uma pasta Parquet para o Blob); o download inteiro de uma pasta é o que faltava, e só apareceu porque agora **cada job também lê** uma camada do Blob, não só escreve nela.

---

## O que você vai descobrir

### 1. Verificar "a camada existe" no Blob é diferente de verificar no disco

No laboratório 100% local, a checagem era `if not origem.exists()`. Aqui, um prefixo de Blob não "existe" no sentido de um diretório — ele só existe **enquanto houver pelo menos um blob com esse nome no começo**. `prefixo_existe()` resolve isso listando o prefixo e checando se a lista veio vazia. É uma pequena diferença de modelo mental que derruba quem tenta portar código de sistema de arquivos para objeto sem pensar duas vezes.

### 2. A ordem dos jobs importa tanto quanto a ordem das condições de validação

Os exercícios de validação já ensinaram que a ordem de um `F.when` decide qual motivo "ganha" numa linha com dois problemas. Aqui aparece a mesma ideia num nível acima: rodar `02_silver_limpeza.py` antes de `01_bronze_ingestao.py` não produz um resultado errado silencioso — produz um `SystemExit` imediato, porque o prefixo `bronze/pluviometria` não existe ainda. É a mesma filosofia (falhar alto e claro) aplicada à ordem dos jobs, não só à validação de uma linha.

### Números da execução

```
Bronze:    924 leituras (identico ao CSV bruto)
Silver:    920 apos dedup (-4 duplicatas), 885 aprovadas, 35 rejeitadas
Gold:      885 linhas no fato, 576 seco / 165 moderado / 144 forte

motivos de rejeicao (Silver):
  chuva fora da faixa (0 a 200)      18
  umidade fora da faixa (0 a 100)    10
  municipio invalida ou ausente       7
```

Total trimestral de chuva por região (mm, decrescente):

```
Vale do Itajai            2160.4
Norte Catarinense         1414.0
Grande Florianopolis      1278.9
Serrana                    528.4
Oeste Catarinense          493.2
Sul Catarinense            379.4
```

---

## Instalação

Guia completo no [SETUP.md](SETUP.md). Resumo:

```bash
docker compose up -d --wait                                   # floci-az
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" azure-storage-blob
.\.venv\Scripts\python.exe verificar_ambiente.py
```

---

## Passo a passo para rodar

```powershell
.\.venv\Scripts\python.exe 00_conectar_blob.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_limpeza.py
.\.venv\Scripts\python.exe 03_gold_agregados.py
```

Ou, depois de completar os cinco exercícios: `python executar_pipeline.py`.

### O que há em cada arquivo

| Arquivo | Camada | Função |
| --- | --- | --- |
| [`comum.py`](comum.py) | — | sessão Spark + cliente do Blob + `subir_pasta`/`baixar_pasta`/`prefixo_existe` |
| [`00_conectar_blob.py`](00_conectar_blob.py) | — | conectar, subir, listar, baixar, apagar — sem Spark |
| [`01_bronze_ingestao.py`](01_bronze_ingestao.py) | Bronze | gera e semeia o Blob, lê, acrescenta proveniência (**exercício 1**), sobe |
| [`02_silver_limpeza.py`](02_silver_limpeza.py) | Silver | baixa a Bronze do Blob, normaliza (**exercício 2**), valida (**exercício 3**), sobe |
| [`03_gold_agregados.py`](03_gold_agregados.py) | Gold | baixa a Silver, classifica e junta com região (**exercício 4**), resume (**exercício 5**), sobe |
| [`executar_pipeline.py`](executar_pipeline.py) | — | roda os três jobs em sequência |
| [`EXERCICIOS_CHUVA.md`](EXERCICIOS_CHUVA.md) | — | enunciados, resposta esperada, gabarito |

---

## Os três jobs

### Bronze

Gera `dados/chuva_sc.csv`, sobe para `fonte/chuva_sc.csv` no Blob (uma vez), baixa de volta, lê com schema fixo, acrescenta proveniência e sobe o resultado para `bronze/pluviometria/`.

### Silver

Checa que `bronze/pluviometria/` existe no Blob, baixa a pasta inteira para `camadas/bronze/pluviometria/`, lê como Parquet, normaliza e valida, sobe `silver/pluviometria/` (aprovadas) e `silver/pluviometria_rejeitada/` (com motivo).

### Gold

Checa `silver/pluviometria/`, baixa, classifica cada leitura e junta com a tabela de região (gerada e semeada por este próprio job, sob `referencia/municipios_sc.csv`), agrega por região, sobe `gold/fato_pluviometria/` e `gold/resumo_regiao/`.

---

## Se algo der errado

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `Bronze nao encontrada no Blob` | job 1 não rodou, ou rodou contra outro container | rode `python 01_bronze_ingestao.py` primeiro; confira `AZURE_STORAGE_CONTAINER` |
| `Silver nao encontrada no Blob` | mesma causa, um job adiante | rode os três na ordem |
| `ServiceRequestError: Connection refused` | o floci-az não está rodando | `docker compose up -d --wait` |
| job para com `AttributeError: 'NoneType' object has no attribute ...` | um `# TODO` ainda não foi completado | é o exercício — veja o [EXERCICIOS_CHUVA.md](EXERCICIOS_CHUVA.md) |
| erro de gateway do py4j, `HADOOP_HOME` ausente | mesmas causas da AULA03 | veja a tabela do [README da AULA03](../../AULA03/PYSPARK-BASICO/README.md#se-algo-der-errado) |

---

## O que levar disso para o trabalho

Este laboratório não introduz uma lição nova — combina duas que já apareceram separadas. A validação com `F.coalesce` + `F.when` continua igual desde a AULA04; a separação em jobs por camada continua igual ao laboratório local. O que muda é só a superfície de contato entre as duas: quando "ler a camada anterior" deixa de ser `spark.read.parquet(pasta)` e passa a ser "baixar do armazenamento remoto, depois ler localmente", cada ponto de verificação (a camada existe? o download terminou? o conteúdo bate?) precisa ser escrito explicitamente, porque o Spark não vai fazer isso por você contra um Blob.

É por isso que pipelines de produção reais quase sempre usam um orquestrador (Airflow, Dagster) em vez do `executar_pipeline.py` deste laboratório: a parte que aqui é "rode os três jobs na ordem certa e torça" vira, em produção, sensores que checam se a camada anterior está pronta antes de disparar a próxima — a mesma pergunta que `prefixo_existe()` responde aqui, só que automatizada.
