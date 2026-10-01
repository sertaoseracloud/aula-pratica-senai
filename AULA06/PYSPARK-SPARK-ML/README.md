# Laboratório 14 — Spark ML dentro do ELT medalhão

**Duração não aferida.** O laboratório depende do ambiente PySpark da AULA03.

Este laboratório combina dois assuntos: o dado é carregado primeiro em uma camada **Bronze**, preparado na **Silver** e transformado em previsões ou segmentos na **Gold**; as transformações de ML usam o Spark MLlib, que faz parte do PySpark. Tudo roda localmente, com dados sintéticos reproduzíveis.

São três problemas de áreas diferentes:

| Caso de uso | Área | Pergunta | Tipo de ML | Algoritmo |
| --- | --- | --- | --- | --- |
| Risco de churn | Assinaturas / telecom | Quem pode cancelar? | Classificação supervisionada | Regressão logística |
| Demanda de energia | Energia / operações | Quanta energia será consumida? | Regressão supervisionada | Random Forest |
| Segmentos de clientes | Varejo / marketing | Que perfis de comportamento existem? | Clusterização não supervisionada | K-Means |

As fontes são artificiais e **não servem para decisões reais**: foram criadas apenas para que o laboratório rode sem baixar datasets nem compartilhar dados pessoais. A semente aleatória fixa torna os resultados reproduzíveis.

## O que você vai praticar

- Reconhecer se a pergunta é classificação, regressão ou clusterização.
- Preparar um vetor `features` com `VectorAssembler` e, para K-Means, normalizar escalas com `StandardScaler`.
- Encadear preparação e estimador num `Pipeline` do Spark ML.
- Separar treino e teste nos casos supervisionados e avaliar com métricas adequadas.
- Gravar previsões, métricas e modelos persistidos como produtos da camada Gold.
- Reexecutar cada camada como um job independente, lendo o Parquet deixado pela anterior.
- Completar três exercícios guiados de Spark ML com enunciado, saída esperada e gabarito.

## ELT e as camadas

Neste exemplo, **Extract** lê as fontes CSV sintéticas, **Load** preserva os dados na Bronze e **Transform** acontece no Spark nas camadas seguintes. É um fluxo ELT: carrega-se o dado bruto antes de aplicar a transformação. A Silver valida os campos e define as entradas que cada caso usa. A Gold treina e aplica modelos, grava as previsões e salva os modelos para reuso.

| Camada | Job | Conteúdo |
| --- | --- | --- |
| Bronze | `01_bronze_ingestao.py` | CSVs originais copiados para Parquet, sem limpeza de negócio |
| Silver | `02_silver_preparacao.py` | registros válidos, tipos inferidos, duplicatas removidas e feature derivada do varejo |
| Gold | `03_gold_modelos.py` | previsões, segmentos, métricas e modelos Spark ML salvos |

O treinamento é uma transformação da Gold neste laboratório didático. Em produção, o treinamento costuma ter cadência própria e versionamento explícito; a inferência pode ser um job separado. Separar os três casos em jobs independentes também seria razoável quando equipes, janelas de execução ou ciclos de atualização forem diferentes.

Este laboratório é um exercício: as funções `classificar_churn`, `prever_demanda` e `segmentar_clientes` em `03_gold_modelos.py` vêm com `TODO`s. Complete-as seguindo [EXERCICIOS_SPARK_ML.md](EXERCICIOS_SPARK_ML.md), que contém os enunciados, as saídas esperadas e o gabarito comentado.

## Instalação

O PySpark já inclui Spark MLlib; não instale `scikit-learn` para estes exemplos. Requisitos: Python 3.8 a 3.12 (3.11 recomendado), Java 8, 11 ou 17 e `pyspark==3.5.3`. No Windows, Hadoop exige `winutils.exe` e `hadoop.dll` até para gravar Parquet local.

Veja o guia completo em [SETUP.md](SETUP.md). Resumo no Windows:

```powershell
cd AULA06\PYSPARK-SPARK-ML
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"

# Copie hadoop/ de uma aula anterior ou siga o Passo 4 do SETUP.md.
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Se o ambiente da AULA03 já estiver pronto, reutilize o Python, Java e Hadoop configurados lá. Não é necessário instalar nada além do PySpark.

## Executar

Na ordem, para acompanhar cada camada:

```powershell
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
.\.venv\Scripts\python.exe 03_gold_modelos.py
```

Rode Bronze e Silver normalmente. O job Gold para nos `TODO`s até os três exercícios serem resolvidos. Depois, rode o pipeline inteiro, mantendo um processo separado por job:

```powershell
.\.venv\Scripts\python.exe executar_pipeline.py
```

Cada execução substitui suas próprias saídas da camada correspondente. Os CSVs sintéticos em `dados/` só são gerados se ainda não existirem; para voltar ao estado inicial, remova `dados/` e `camadas/` manualmente.

## Como ler os três modelos

### Classificação: churn

`cancelou` é a variável-alvo binária. `VectorAssembler` junta tempo de assinatura, mensalidade, chamados e duração do contrato num vetor. `LogisticRegression` é ajustada em 80% das linhas; os outros 20% ficam como teste. A Gold grava a classe prevista e o vetor de probabilidades por cliente.

`area_under_roc` mede a capacidade de ordenar positivos acima de negativos; `acuracia` é a fração total de classificações corretas. Com classes desbalanceadas, acurácia sozinha pode parecer boa sem encontrar os cancelamentos. Em produção, avalie também precisão, recall, matriz de confusão e o custo de cada erro.

### Regressão: energia

`demanda_mwh` é um valor contínuo, então o estimador prevê uma quantidade, não uma categoria. O Random Forest combina árvores para capturar relações não lineares entre temperatura, dia da semana, feriado e ocupação. `RMSE` penaliza mais os erros grandes; `MAE` expressa o erro absoluto médio na unidade MWh.

### Clusterização: varejo

Não existe coluna-alvo. `StandardScaler` é importante porque gasto, frequência e recência têm escalas distintas; sem normalização, gasto em reais poderia dominar as distâncias do K-Means. O `segmento` é um identificador arbitrário, não uma categoria com ordem ou significado já conhecido. Examine os centróides e os resumos por cluster antes de atribuir nomes de negócio.

O exemplo registra WSSSE (*within-set sum of squared errors*), custo da distância dos pontos aos centróides. Menor não significa automaticamente melhor: essa medida tende a cair quando `k` cresce. Uma escolha de `k` real compararia vários valores, estabilidade e utilidade dos segmentos; Silhouette é uma métrica diferente e não é calculada por este job.

## Saídas

```text
camadas/
  bronze/{assinaturas,demanda_energia,clientes_varejo}/
  silver/{assinaturas,demanda_energia,clientes_varejo}/
  gold/
    metricas/
    previsoes_churn/
    previsoes_energia/
    segmentos_varejo/
    modelos/{churn_logistic_regression,energia_random_forest,varejo_kmeans}/
```

As previsões e métricas são dados de consumo; os diretórios em `modelos/` são artefatos serializados do Spark ML e podem ser carregados com `PipelineModel.load(caminho)`. Os detalhes de cada caminho estão em `.gitignore`, pois são saídas geradas.

## Exercícios

Resolva [os três exercícios guiados de Spark ML](EXERCICIOS_SPARK_ML.md): classificação de churn, regressão da demanda e clusterização de clientes. Cada exercício indica os campos de entrada, o algoritmo, a métrica, a saída esperada e inclui um gabarito comentado. Depois, experimente alterar parâmetros e features sem usar o conjunto de teste repetidamente para escolher o modelo.

## Arquivos

| Arquivo | Papel |
| --- | --- |
| `comum.py` | sessão Spark local, ambiente Windows e caminhos |
| `01_bronze_ingestao.py` | geração e ingestão das fontes sintéticas |
| `02_silver_preparacao.py` | validação e preparação por caso de uso |
| `03_gold_modelos.py` | pipelines ML, métricas, previsões e persistência |
| `executar_pipeline.py` | executa os três jobs em sequência |
| `verificar_ambiente.py` | confere Python, PySpark, Spark ML, Java e winutils |
| `EXERCICIOS_SPARK_ML.md` | enunciados, saídas esperadas e gabaritos comentados |
| `SETUP.md` | preparação detalhada do ambiente |
