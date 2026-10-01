# Laboratório 15 — Spark ML: fraude, texto e recomendação

**Segunda parte da AULA06.** Este laboratório continua o ELT medalhão com três aplicações diferentes das da [parte 1](../PYSPARK-SPARK-ML/README.md): detectar transações fraudulentas, classificar sentimento em texto e recomendar produtos a usuários.

| Aplicação | Área | Problema ML | Técnica Spark ML |
| --- | --- | --- | --- |
| Fraude em transações | Finanças / pagamentos | Classificação binária com classe rara | Random Forest Classifier |
| Sentimento de avaliações | NLP / comércio eletrônico | Classificação de texto | RegexTokenizer + HashingTF + Naive Bayes |
| Recomendação de produtos | Varejo / personalização | Filtragem colaborativa | ALS |

Os dados são sintéticos, reproduzíveis e não representam usuários ou transações reais. São gerados localmente para não exigir credenciais, downloads ou dependências além do PySpark.

## O que muda em relação à parte 1

A parte 1 apresenta regressão logística, regressão de valores contínuos e K-Means. Aqui entram desafios que pedem cuidados diferentes:

- **Fraude:** poucas transações são positivas; acurácia pode esconder um detector que não encontra nenhuma fraude. A métrica principal do exercício é AUPRC, junto com recall da classe positiva.
- **Texto:** o modelo não recebe texto diretamente. O `Pipeline` transforma frase em tokens, remove palavras comuns, converte tokens em vetor numérico e aplica Naive Bayes.
- **Recomendação:** não há rótulo de fraude ou sentimento; o dado é uma matriz esparsa de notas usuário-produto. ALS aprende fatores latentes e gera recomendações Top-3.

## ELT medalhão

| Camada | Job | Responsabilidade |
| --- | --- | --- |
| Bronze | `01_bronze_ingestao.py` | Gera e carrega os três CSVs originais para Parquet, sem filtrar linhas |
| Silver | `02_silver_preparacao.py` | Remove duplicatas e separa registros válidos segundo regras simples |
| Gold | `03_gold_aplicacoes.py` | Treina modelos, avalia previsões e persiste previsões, recomendações e modelos |

Cada job sobe sua própria `SparkSession`, lê a saída física da camada anterior e termina. A Gold contém três funções `TODO` para os exercícios.

## Instalação e execução

Reutilize os requisitos da parte 1: Python 3.8 a 3.12 (3.11 recomendado), Java 8, 11 ou 17, `pyspark==3.5.3` e, no Windows, `winutils.exe` mais `hadoop.dll`. Spark MLlib vem junto com PySpark.

As instruções detalhadas estão em [SETUP.md](SETUP.md). Com o ambiente pronto:

```powershell
cd AULA06\PYSPARK-SPARK-ML-PARTE2
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
.\.venv\Scripts\python.exe 03_gold_aplicacoes.py
```

Resolva os três TODOs antes de executar `executar_pipeline.py`. Os exercícios, as saídas esperadas e os gabaritos estão em [EXERCICIOS_SPARK_ML_PARTE2.md](EXERCICIOS_SPARK_ML_PARTE2.md).

## Saídas Gold

```text
camadas/gold/
  fraudes/                 # previsões de fraude no conjunto de teste
  sentimentos/             # sentimento previsto por avaliação
  notas_previstas/         # notas reais e estimadas no teste ALS
  recomendacoes_top3/      # três produtos recomendados por usuário
  metricas/                # AUPRC, recall, acurácia e RMSE
  modelos/
    fraude_rf/
    sentimento_nb/
    als/
```

Modelos e previsões são artefatos de estudo, não modelos prontos para uso operacional. Na fraude real, também seriam necessários custo de falsos positivos, limiar ajustável, monitoramento de drift e revisão humana. Para recomendação em produção, seria preciso tratar usuários/produtos novos (cold start), privacidade e avaliação online.

## Arquivos

| Arquivo | Papel |
| --- | --- |
| `comum.py` | Configuração local da SparkSession e caminhos das camadas |
| `01_bronze_ingestao.py` | Gera e ingere transações, avaliações e notas sintéticas |
| `02_silver_preparacao.py` | Valida as três fontes |
| `03_gold_aplicacoes.py` | Três exercícios de ML e persistência das saídas |
| `executar_pipeline.py` | Executa cada camada como processo separado |
| `EXERCICIOS_SPARK_ML_PARTE2.md` | Enunciados, resultados esperados e gabaritos |
| `SETUP.md` | Preparação do ambiente |
