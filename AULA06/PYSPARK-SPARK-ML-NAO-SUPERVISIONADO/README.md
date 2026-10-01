# Laboratório 18 — aprendizado não supervisionado com Spark ML

Esta trilha é **exclusivamente não supervisionada**: os exemplos não possuem nem usam coluna-alvo. O objetivo é descobrir estrutura no dado e representar os perfis de forma mais compacta.

| Aplicação | Pergunta | Técnica |
| --- | --- | --- |
| Segmentação de clientes | Que grupos de comportamento aparecem nos dados? | K-Means |
| Redução de dimensionalidade | Como representar vários atributos em duas dimensões? | PCA |

Os perfis são sintéticos e não incluem identificadores de grupo. As distribuições de origem criam padrões para que os algoritmos encontrem; o rótulo de cada perfil não é entregue nem usado pelo pipeline.

## Sem alvo, sem treino supervisionado

No aprendizado supervisionado, um modelo recebe exemplos e respostas corretas. Aqui não existe uma coluna `label`: K-Means agrupa pontos por distância e PCA encontra eixos que explicam variação. O número de cluster é um identificador arbitrário, não uma resposta correta.

## ELT medalhão

| Camada | Job | Responsabilidade |
| --- | --- | --- |
| Bronze | `01_bronze_ingestao.py` | Gera e preserva atributos comportamentais sem rótulos |
| Silver | `02_silver_preparacao.py` | Valida atributos, audita rejeições e cria gasto médio por compra |
| Gold | `03_gold_nao_supervisionado.py` | Gera segmentos K-Means e projeção PCA, sem alvo |

## Executar

Requisitos: Python 3.8–3.12 (3.11 recomendado), Java 8/11/17 e `pyspark==3.5.3`; no Windows, `winutils.exe` e `hadoop.dll`. Veja [SETUP.md](SETUP.md).

```powershell
cd AULA06\PYSPARK-SPARK-ML-NAO-SUPERVISIONADO
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_preparacao.py
```

Complete os exercícios em [EXERCICIOS_NAO_SUPERVISIONADO.md](EXERCICIOS_NAO_SUPERVISIONADO.md) antes de rodar o Gold ou `executar_pipeline.py`.

## Saídas Gold

```text
camadas/gold/
  segmentos_clientes/       # cluster atribuído mais os atributos originais
  projecao_clientes_pca/    # coordenadas pc1 e pc2 sem classe-alvo
  metricas/                 # WSSSE do K-Means
  modelos/{clientes_kmeans,clientes_pca}/
```

## Arquivos

| Arquivo | Papel |
| --- | --- |
| `01_bronze_ingestao.py` | Gera perfis sem rótulo e grava Bronze |
| `02_silver_preparacao.py` | Valida e cria feature derivada |
| `03_gold_nao_supervisionado.py` | Exercícios K-Means e PCA |
| `EXERCICIOS_NAO_SUPERVISIONADO.md` | Enunciados e gabaritos |
| `SETUP.md` | Instalação e execução |
