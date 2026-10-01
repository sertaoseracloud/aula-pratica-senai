# Laboratório 17 — aprendizado supervisionado com Spark ML

Esta trilha é **exclusivamente supervisionada**: todo modelo aprende a partir de uma coluna-alvo conhecida. Ela cobre os dois tipos mais comuns de predição: classificação de uma categoria e regressão de um valor contínuo.

| Caso de uso | Alvo conhecido | Problema | Estimador |
| --- | --- | --- | --- |
| Cancelamento de assinatura | `cancelou` (0/1) | Classificação binária | Regressão logística |
| Demanda de energia | `demanda_mwh` | Regressão | Random Forest Regressor |

Os dados são sintéticos, reproduzíveis e não devem ser usados para decisões reais.

## Ideia central

No aprendizado supervisionado, exemplos de treino contêm tanto as features quanto a resposta correta (`label`). O algoritmo ajusta um modelo para prever esse alvo em exemplos que não viu. Os dois exercícios separam treino e teste; não inclua o alvo em `VectorAssembler`, pois isso causaria vazamento de dados.

## ELT medalhão

| Camada | Job | Responsabilidade |
| --- | --- | --- |
| Bronze | `01_bronze_ingestao.py` | Gera e preserva CSVs rotulados em Parquet |
| Silver | `02_silver_preparacao.py` | Valida os campos e grava aprovados e rejeitados |
| Gold | `03_gold_supervisionado.py` | Ajusta classificação e regressão; publica previsões, modelos e métricas |

Cada camada é um processo independente. Silver confere a conciliação entre registros aprovados e rejeitados.

## Executar

Requisitos: Python 3.8–3.12 (3.11 recomendado), Java 8/11/17 e `pyspark==3.5.3`; no Windows, `winutils.exe` e `hadoop.dll`. Veja [SETUP.md](SETUP.md).

```powershell
cd AULA06\PYSPARK-SPARK-ML-SUPERVISIONADO
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
```

Complete os dois exercícios em [EXERCICIOS_SUPERVISIONADO.md](EXERCICIOS_SUPERVISIONADO.md), depois rode `03_gold_supervisionado.py` ou `executar_pipeline.py`.

## Saídas Gold

```text
camadas/gold/
  previsoes_churn/
  previsoes_demanda/
  metricas/
  modelos/{churn_logistic_regression,demanda_random_forest}/
```

## Arquivos

| Arquivo | Papel |
| --- | --- |
| `01_bronze_ingestao.py` | Gera duas fontes com rótulos supervisionados |
| `02_silver_preparacao.py` | Valida e reconcilia dados aprovados/rejeitados |
| `03_gold_supervisionado.py` | Exercícios de classificação e regressão |
| `EXERCICIOS_SUPERVISIONADO.md` | Enunciados, respostas esperadas e gabaritos |
| `SETUP.md` | Instalação e execução |
