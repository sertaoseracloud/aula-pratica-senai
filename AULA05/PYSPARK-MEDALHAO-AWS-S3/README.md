# Laboratório 13 — arquitetura medalhão com AWS S3

Mesma combinação do laboratório [`PYSPARK-MEDALHAO-AZURE-BLOB`](../PYSPARK-MEDALHAO-AZURE-BLOB/README.md) ao lado — arquitetura medalhão (três jobs, um por camada) mais armazenamento em nuvem — trocando Azure por AWS. Cada camada (Bronze, Silver, Gold) é um prefixo num bucket S3, emulado localmente pelo [floci](https://floci.io/aws/). O dado é coleta de resíduos sólidos (toneladas totais e recicláveis) de um trimestre, nas mesmas dez cidades de Santa Catarina dos outros laboratórios.

**Este laboratório é um exercício.** `00_conectar_s3.py` vem pronto; os três jobs têm cinco funções incompletas ao todo — enunciados, resposta esperada e gabarito em [`EXERCICIOS_RESIDUOS.md`](EXERCICIOS_RESIDUOS.md).

## Sumário

- [O que muda em relação aos laboratórios anteriores](#o-que-muda-em-relação-aos-laboratórios-anteriores)
- [O que você vai descobrir](#o-que-você-vai-descobrir)
- [Instalação](#instalação)
- [Passo a passo para rodar](#passo-a-passo-para-rodar)
- [Os três jobs](#os-três-jobs)
- [Se algo der errado](#se-algo-der-errado)
- [O que levar disso para o trabalho](#o-que-levar-disso-para-o-trabalho)

---

## O que muda em relação aos laboratórios anteriores

Este é o quarto laboratório da série de exercícios de nuvem/medalhão, e o único conceito realmente novo aqui é mecânico: o S3 pagina listagens de mais de 1000 chaves, o Blob não (`list_blobs` do Azure já devolve um iterador que pagina sozinho por trás dos panos). Por isso `baixar_pasta()` neste `comum.py` usa explicitamente um `paginator` do boto3:

```python
paginador = s3.get_paginator("list_objects_v2")
for pagina in paginador.paginate(Bucket=BUCKET, Prefix=f"{prefixo}/"):
    for objeto in pagina.get("Contents", []):
        ...
```

Para as camadas deste laboratório (poucas dezenas de arquivos Parquet) isso nunca chega perto do limite de 1000 — mas o código já está escrito do jeito que continuaria funcionando se chegasse. É uma diferença de detalhe de API entre dois SDKs que, sem ler a documentação de cada um, é fácil não perceber até o dia em que uma camada Gold crescer.

Fora isso, a estrutura é idêntica ao laboratório do Blob: três jobs, cada um verificando que a camada anterior existe (`prefixo_existe()`) antes de baixá-la (`baixar_pasta()`), processar, e subir o resultado (`subir_pasta()`).

---

## O que você vai descobrir

### 1. Um limiar de validação "generoso" pode não ser generoso o bastante

Das 64 leituras rejeitadas na Silver, 47 caem por `toneladas_total` fora de `[0, 500]` — bem mais do que a sujeira injetada de propósito explicaria sozinha. Cidades grandes como Joinville partem de uma base de ~480 toneladas/dia, e a variação natural do dado às vezes empurra esse valor "limpo" para além de 500 por puro acaso. O limiar não estava errado por má vontade — estava errado porque foi escolhido sem checar a distribuição real da coluna primeiro.

### 2. Uma regra pode nunca vencer a disputa de prioridade, mesmo sendo necessária

`reciclavel fora da faixa fisica (0 a 500)` nunca aparece como motivo registrado — toda vez que o reciclável sai da faixa, uma das duas regras anteriores (município ou reciclável > total) já capturou a linha primeiro. A regra continua correta: ela é a rede de segurança para o dia em que as outras duas não pegarem o problema.

### Números da execução

```
Bronze:    921 leituras (identico ao CSV bruto)
Silver:    920 apos dedup (-1 duplicata), 856 aprovadas, 64 rejeitadas
Gold:      856 linhas no fato, 258 baixa / 577 media / 21 alta (taxa de reciclagem)

motivos de rejeicao (Silver):
  total fora da faixa fisica (0 a 500)         47
  reciclavel maior que o total                  12
  municipio invalida ou ausente                  5
  reciclavel fora da faixa fisica (0 a 500)      0
```

Total trimestral coletado por região (toneladas, decrescente):

```
Grande Florianopolis     53153.0
Vale do Itajai           51770.7
Norte Catarinense        40463.7
Oeste Catarinense        16122.6
Sul Catarinense          15432.6
Serrana                  11001.5
```

---

## Instalação

Guia completo no [SETUP.md](SETUP.md). Resumo:

```bash
docker compose up -d --wait                                   # floci
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" boto3
.\.venv\Scripts\python.exe verificar_ambiente.py
```

---

## Passo a passo para rodar

```powershell
.\.venv\Scripts\python.exe 00_conectar_s3.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_limpeza.py
.\.venv\Scripts\python.exe 03_gold_agregados.py
```

Ou, depois de completar os cinco exercícios: `python executar_pipeline.py`.

### O que há em cada arquivo

| Arquivo | Camada | Função |
| --- | --- | --- |
| [`comum.py`](comum.py) | — | sessão Spark + cliente S3 + `subir_pasta`/`baixar_pasta`/`prefixo_existe` |
| [`00_conectar_s3.py`](00_conectar_s3.py) | — | conectar, subir, listar, baixar, apagar — sem Spark |
| [`01_bronze_ingestao.py`](01_bronze_ingestao.py) | Bronze | gera e semeia o S3, lê, acrescenta proveniência (**exercício 1**), sobe |
| [`02_silver_limpeza.py`](02_silver_limpeza.py) | Silver | baixa a Bronze do S3, normaliza (**exercício 2**), valida (**exercício 3**), sobe |
| [`03_gold_agregados.py`](03_gold_agregados.py) | Gold | baixa a Silver, calcula taxa de reciclagem e junta com região (**exercício 4**), resume (**exercício 5**), sobe |
| [`executar_pipeline.py`](executar_pipeline.py) | — | roda os três jobs em sequência |
| [`EXERCICIOS_RESIDUOS.md`](EXERCICIOS_RESIDUOS.md) | — | enunciados, resposta esperada, gabarito |

---

## Os três jobs

### Bronze

Gera `dados/residuos_sc.csv`, sobe para `fonte/residuos_sc.csv` no S3 (uma vez), baixa de volta, lê com schema fixo, acrescenta proveniência e sobe o resultado para `bronze/residuos/`.

### Silver

Checa que `bronze/residuos/` existe no S3, baixa a pasta inteira, normaliza e valida, sobe `silver/residuos/` (aprovadas) e `silver/residuos_rejeitada/` (com motivo).

### Gold

Checa `silver/residuos/`, baixa, calcula a taxa de reciclagem por leitura, junta com a tabela de região (gerada e semeada por este job, sob `referencia/municipios_sc.csv`), agrega por região, sobe `gold/fato_residuos/` e `gold/resumo_regiao/`.

---

## Se algo der errado

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `Bronze nao encontrada no S3` | job 1 não rodou, ou rodou contra outro bucket | rode `python 01_bronze_ingestao.py` primeiro; confira `AWS_S3_BUCKET` |
| `Silver nao encontrada no S3` | mesma causa, um job adiante | rode os três na ordem |
| `EndpointConnectionError` | o floci não está rodando | `docker compose up -d --wait` |
| job para com `AttributeError: 'NoneType' object has no attribute ...` | um `# TODO` ainda não foi completado | é o exercício — veja o [EXERCICIOS_RESIDUOS.md](EXERCICIOS_RESIDUOS.md) |
| erro de gateway do py4j, `HADOOP_HOME` ausente | mesmas causas da AULA03 | veja a tabela do [README da AULA03](../../AULA03/PYSPARK-BASICO/README.md#se-algo-der-errado) |

---

## O que levar disso para o trabalho

Quatro laboratórios de validação depois (temperaturas, energia, qualidade do ar, chuva, resíduos — este é o quinto, na verdade), o padrão `F.coalesce(regra, F.lit(False))` mais uma cadeia de `F.when` deixou de ser "a solução deste exercício" para virar **o jeito de fazer isso**, ponto. É esse tipo de repetição — a mesma lógica reaparecendo em cinco domínios de dado completamente diferentes — que separa uma técnica que se aprendeu de uma técnica que só se copiou uma vez.

A lição nova deste laboratório é mais sutil: o limiar `[0, 500]` do exercício 3 pareceu razoável na hora de escrever a regra, e ainda assim rejeitou 47 linhas de dado plausível, não sujo. Uma regra de validação não é só "que condição escrever" — é também "que número usar nela", e esse número tem que vir de olhar a distribuição real da coluna, não da intuição de quem escreveu a regra às 9h da manhã sem ter visto o dado ainda.
