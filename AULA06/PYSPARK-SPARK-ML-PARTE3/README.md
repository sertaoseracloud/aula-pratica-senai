# Laboratório 16 — manutenção preditiva com Spark ML

**Terceira parte da AULA06.** O laboratório aplica Spark ML à manutenção preditiva: com leituras sintéticas de sensores industriais, o modelo estima se uma máquina terá falha nas próximas 24 horas. A novidade em relação às partes anteriores é a seleção de hiperparâmetros com `CrossValidator` e `ParamGridBuilder`.

| Elemento | Aplicação |
| --- | --- |
| Área | Manutenção industrial / operações |
| Pergunta | A leitura dos sensores indica risco de falha nas próximas 24 horas? |
| Problema ML | Classificação binária supervisionada |
| Estimador | `GBTClassifier` |
| Seleção | Validação cruzada com duas dobras e grade pequena de parâmetros |
| Métricas | AUC ROC na validação cruzada e no teste; recall no teste |

Os dados são sintéticos, reprodutíveis e não servem para decisões de segurança ou manutenção reais.

## O que o exercício ensina

- Preservar as leituras originais na Bronze.
- Validar faixas de sensores na Silver e gravar rejeições com motivo.
- Reservar um conjunto de teste antes de selecionar parâmetros.
- Usar `Pipeline`, `ParamGridBuilder` e `CrossValidator` para comparar configurações somente no conjunto de treino.
- Avaliar o melhor modelo uma vez no teste e persistir previsões e modelo.

O conjunto de teste não participa da validação cruzada nem da escolha do melhor estimador. Essa separação evita relatar no teste o mesmo resultado usado para ajustar os hiperparâmetros.

## ELT medalhão

| Camada | Job | Responsabilidade |
| --- | --- | --- |
| Bronze | `01_bronze_ingestao.py` | Gera 2.400 leituras e preserva o CSV em Parquet |
| Silver | `02_silver_preparacao.py` | Remove duplicatas, valida sensores e grava rejeições com motivo |
| Gold | `03_gold_manutencao.py` | Ajusta modelos candidatos, avalia o melhor no teste e publica previsões |

Cada job sobe sua própria SparkSession e lê a camada anterior do disco. Silver confere a conciliação: linhas aprovadas + rejeitadas devem igualar as linhas normalizadas.

## Instalação e execução

O laboratório usa o mesmo ambiente Spark ML local das partes anteriores: Python 3.8–3.12 (3.11 recomendado), Java 8/11/17 e `pyspark==3.5.3`. No Windows, também são necessários `winutils.exe` e `hadoop.dll`. Veja [SETUP.md](SETUP.md) para os passos completos.

Execute Bronze e Silver primeiro:

```powershell
cd AULA06\PYSPARK-SPARK-ML-PARTE3
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
```

Complete `treinar_modelo_manutencao` conforme [EXERCICIOS_SPARK_ML_PARTE3.md](EXERCICIOS_SPARK_ML_PARTE3.md), então rode:

```powershell
.\.venv\Scripts\python.exe 03_gold_manutencao.py
```

O pipeline completo, com um processo por camada, é `.\.venv\Scripts\python.exe executar_pipeline.py` depois de resolver o exercício.

## Saídas Gold

```text
camadas/gold/
  risco_falha_maquina/       # previsões para leituras do conjunto de teste
  metricas/                  # AUC da validação cruzada, AUC teste e recall teste
  modelos/manutencao_gbt/    # melhor PipelineModel encontrado na validação cruzada
```

O recall é importante quando deixar passar uma falha custa caro, mas não é a história toda: um limiar agressivo pode gerar muitos alarmes falsos. Em um sistema real, a equipe teria de calibrar o limiar e validar o modelo com dados temporais e custos operacionais reais.

## Arquivos

| Arquivo | Papel |
| --- | --- |
| `comum.py` | Sessão local e caminhos das camadas |
| `01_bronze_ingestao.py` | Geração e ingestão das leituras sintéticas |
| `02_silver_preparacao.py` | Validação, rejeições auditáveis e conciliação |
| `03_gold_manutencao.py` | Exercício de seleção e avaliação de modelo |
| `executar_pipeline.py` | Encadeia os três jobs como processos separados |
| `EXERCICIOS_SPARK_ML_PARTE3.md` | Enunciado, resposta esperada e gabarito |
| `SETUP.md` | Instalação e erros comuns |
