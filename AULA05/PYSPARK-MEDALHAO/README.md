# Laboratório 11 — arquitetura medalhão: um job por camada

Os laboratórios anteriores (AULA03 e AULA04) resolveram Extract/Transform/Load como **um script**, com uma função por etapa. Este muda a unidade: cada camada da arquitetura medalhão — **Bronze, Silver, Gold** — é um **job independente**, que roda sozinho, lê a saída do job anterior no disco e não sabe nada sobre quem vai consumir a sua saída. É assim que pipelines de dado costumam ser organizados de verdade, um por trás de um agendador (Airflow, Dagster, Step Functions) em vez de um script monolítico.

O dado é qualidade do ar (PM2.5, PM10, CO) de um trimestre (jul–set/2024) nas mesmas dez cidades de Santa Catarina dos laboratórios anteriores, simulado com semente fixa.

**Este laboratório é um exercício.** Os três jobs vêm com cinco funções incompletas ao todo — enunciados, resposta esperada e gabarito em [`EXERCICIOS_MEDALHAO.md`](EXERCICIOS_MEDALHAO.md).

## Sumário

- [Antes do código: o vocabulário mínimo](#antes-do-código-o-vocabulário-mínimo)
- [O que você vai descobrir](#o-que-você-vai-descobrir)
- [Instalação](#instalação)
- [Passo a passo para rodar](#passo-a-passo-para-rodar)
- [Job 1 — Bronze](#job-1--bronze)
- [Job 2 — Silver](#job-2--silver)
- [Job 3 — Gold](#job-3--gold)
- [Se algo der errado](#se-algo-der-errado)
- [O que levar disso para o trabalho](#o-que-levar-disso-para-o-trabalho)

---

## Antes do código: o vocabulário mínimo

| Termo | O que é, sem rodeio |
| --- | --- |
| **Arquitetura medalhão** | um jeito de organizar um data lake em camadas de confiabilidade crescente: Bronze (cru), Silver (limpo e validado), Gold (agregado para consumo). Cada camada é uma cópia física do dado, não uma view. |
| **Bronze** | o dado exatamente como chegou da fonte, sem filtrar nada — só com metadado de proveniência (de onde veio, quando chegou). Existe para auditoria e reprocessamento: se uma regra de negócio mudar, você reprocessa a partir da Bronze, não pede o dado de novo à fonte. |
| **Silver** | o dado depois de normalizado, deduplicado e validado — com uma tabela separada para o que foi rejeitado, e o motivo. É a camada em que "confiável" passa a fazer sentido. |
| **Gold** | o dado agregado e enriquecido para uma pergunta de negócio específica — médias por região, contagens por categoria. Um dashboard lê a Gold, nunca a Bronze. |
| **Job** | um programa que roda uma vez, faz um trabalho definido e termina. Neste laboratório, cada job é um arquivo `.py` com sua própria `SparkSession`, que sobe e desce a cada execução — não uma função dentro de um script maior. |

### Por que jobs separados, e não um script com três funções

Os exercícios de ETL da AULA04 (Blob e S3) resolveram normalizar/validar/enriquecer/resumir como quatro funções dentro do **mesmo** arquivo, chamadas em sequência dentro de um `main()` só. Funciona bem quando o pipeline inteiro é pequeno e sempre roda do início ao fim.

A arquitetura medalhão assume o oposto: **cada camada pode ser reprocessada sozinha**, sem repetir o trabalho das anteriores. Se uma regra de negócio da Gold mudar (por exemplo, o limiar de "moderado" passar de 50 para 35 µg/m³), você roda só `03_gold_agregados.py` de novo — a Bronze e a Silver não mudaram, não precisam rodar de novo. Isso só é possível porque cada camada é gravada em disco como um resultado durável, e o job seguinte lê **esse resultado**, não o script anterior.

O preço dessa separação é que cada job sobe a própria JVM e paga o custo de inicialização do Spark de novo — no laboratório da AULA03, esse custo já era mencionado como boa parte dos "~10 minutos" do exercício. Para um pipeline deste tamanho, o preço é pequeno perto do ganho em isolamento; para um pipeline maior, é exatamente esse cálculo que orquestradores como Airflow existem para gerenciar.

---

## O que você vai descobrir

Todos os números abaixo saíram de uma execução real dos três jobs já resolvidos.

### 1. A Bronze não é "dado bruto guardado à toa"

Das 923 linhas geradas, **todas as 923** entram na Bronze — inclusive as duplicatas e as leituras com PM2.5 maior que PM10 (fisicamente impossível). Só na Silver esse número cai para 889 aprovadas. Se a Bronze filtrasse antecipadamente, a resposta para "por que esse sensor mandou um valor impossível no dia X" desapareceria do sistema — a Silver decide o que é válido, mas só a Bronze sabe o que realmente chegou.

### 2. Uma categoria pode ter zero ocorrências e ainda assim ser necessária

Nenhuma leitura do trimestre caiu na faixa "ruim" (PM2.5 > 50) — as 51 leituras "moderadas" foram o pior caso observado. Isso não torna a categoria "ruim" inútil: ela está no contrato da tabela Gold para o dia em que uma inversão térmica severa ou um incêndio florestal empurrarem o índice para cima, sem exigir alteração de schema nesse momento.

### 3. Duas regiões empatam na contagem e discordam na causa

Sul Catarinense e Norte Catarinense têm exatamente 21 dias moderados-ou-piores cada — mas em Sul Catarinense isso vem de uma única cidade (é a única do grupo), e em Norte Catarinense, 20 dos 21 vêm de uma cidade só (Joinville), mesmo a região tendo duas. Uma tabela agregada por região não mostra essa diferença — é preciso descer um nível (por `municipio`) para ver que "mesma contagem" não significa "mesmo problema".

### Números da execução

```
Bronze:    923 leituras (identico ao CSV bruto)
Silver:    920 apos dedup (-3 duplicatas), 889 aprovadas, 31 rejeitadas
Gold:      889 linhas no fato, 838 bom / 51 moderado / 0 ruim

motivos de rejeicao (Silver):
  co fora da faixa (0 a 50)                    11
  pm25 maior que pm10                           9
  municipio invalida ou ausente                 7
  particulado fora da faixa fisica (0 a 500)    4
```

Média trimestral de PM2.5 por região (µg/m³, decrescente):

```
Sul Catarinense           22.59
Norte Catarinense         19.85
Serrana                   18.03
Vale do Itajai            17.55
Oeste Catarinense         15.42
Grande Florianopolis      14.08
```

---

## Instalação

Guia completo no [SETUP.md](SETUP.md). Resumo (sem Docker, sem SDK de nuvem):

```bash
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
# Windows: baixar hadoop/bin/winutils.exe (Passo 4 do SETUP.md)
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Se você já rodou a AULA03 ou qualquer laboratório da AULA04 nesta máquina, o Python, o Java e o `winutils.exe` já estão prontos.

---

## Passo a passo para rodar

```powershell
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_limpeza.py
.\.venv\Scripts\python.exe 03_gold_agregados.py
```

Cada job para com uma mensagem clara se a camada anterior não existir (`Bronze nao encontrada em ...`). Depois de completar os cinco exercícios, `executar_pipeline.py` roda os três em sequência, como um processo separado cada um — é um atalho de conveniência, não um orquestrador de verdade (veja a docstring do arquivo).

### O que há em cada arquivo

| Arquivo | Camada | Função |
| --- | --- | --- |
| [`comum.py`](comum.py) | — | sessão Spark local + caminhos das três camadas |
| [`01_bronze_ingestao.py`](01_bronze_ingestao.py) | Bronze | gera o CSV, lê, acrescenta proveniência (**exercício 1**), grava |
| [`02_silver_limpeza.py`](02_silver_limpeza.py) | Silver | lê a Bronze, normaliza (**exercício 2**), valida (**exercício 3**), grava validadas + rejeitadas |
| [`03_gold_agregados.py`](03_gold_agregados.py) | Gold | lê a Silver, classifica e junta com região (**exercício 4**), resume por região (**exercício 5**), grava |
| [`executar_pipeline.py`](executar_pipeline.py) | — | roda os três jobs em sequência, cada um como processo separado |
| [`EXERCICIOS_MEDALHAO.md`](EXERCICIOS_MEDALHAO.md) | — | os cinco enunciados, resposta esperada e gabarito |
| [`verificar_ambiente.py`](verificar_ambiente.py) | — | confere Python, Java, PySpark e `winutils` |

---

## Job 1 — Bronze

Lê o CSV gerado (com sujeira proposital), aplica um schema fixo, e a única transformação permitida é **proveniência**: de qual arquivo veio, quando foi ingerido. Grava em `camadas/bronze/qualidade_ar/` sem particionar — ninguém consulta a Bronze de forma seletiva, quem lê é sempre o próximo job inteiro.

## Job 2 — Silver

Lê **a Bronze**, nunca o CSV. Normaliza texto e data, remove duplicata, e separa aprovadas de rejeitadas com o mesmo padrão `F.coalesce(regra, F.lit(False))` + cadeia de `F.when` dos exercícios da AULA04. Grava duas saídas: `camadas/silver/qualidade_ar/` (Parquet, validado) e `camadas/silver/qualidade_ar_rejeitada/` (CSV, com `motivo`).

## Job 3 — Gold

Lê **a Silver**, mais uma tabela de apoio (`municipio -> regiao`) que não passa pelas camadas anteriores — é referência praticamente estática, não um fato que chega todo dia. Classifica cada leitura (`faixa_qualidade`) e agrega por região. Grava `camadas/gold/fato_qualidade_ar/` (particionado por `municipio`) e `camadas/gold/resumo_regiao/`.

---

## Se algo der errado

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `Bronze nao encontrada em ...` | rodou `02_silver_limpeza.py` sem rodar o job 1 antes, ou apagou `camadas/` | rode `python 01_bronze_ingestao.py` primeiro |
| `Silver nao encontrada em ...` | mesma causa, um job adiante | rode os três jobs na ordem |
| job para com `AttributeError: 'NoneType' object has no attribute ...` | um `# TODO` ainda não foi completado | é o exercício, não um bug — veja o [EXERCICIOS_MEDALHAO.md](EXERCICIOS_MEDALHAO.md) |
| `executar_pipeline.py` para no meio | um dos jobs falhou (código de saída != 0) | leia a saída do job que falhou, corrija, rode `executar_pipeline.py` de novo (ele refaz os três) |
| erro de gateway do py4j, `HADOOP_HOME` ausente, `UnsupportedClassVersionError` | mesmas causas da AULA03 (Java, `winutils`, versão do PySpark) | veja a tabela do [README da AULA03](../../AULA03/PYSPARK-BASICO/README.md#se-algo-der-errado) |

---

## O que levar disso para o trabalho

Os laboratórios de ETL da AULA03 e da AULA04 ensinaram a validar com `F.coalesce` e a separar aprovados de rejeitados com motivo — e essa lógica não muda aqui, ela só se move para dentro do job da Silver. O que este laboratório acrescenta é uma pergunta diferente: **onde termina uma unidade de trabalho, e onde começa a próxima?**

Um script único que faz tudo é mais simples de ler na primeira vez, mas acopla o tempo de vida de decisões que deveriam ser independentes: mudar uma regra de agregação na Gold não deveria exigir reingestão da fonte. Separar por camada — com cada uma escrita em disco antes da próxima começar — é o que torna essa independência possível, ao custo de mais um ponto onde um job pode falhar sozinho e precisar de uma mensagem de erro que diga exatamente qual dependência faltou. É por isso que os três jobs deste laboratório verificam explicitamente se a camada anterior existe antes de tentar lê-la, em vez de deixar o Spark estourar um `AnalysisException` sem contexto.
