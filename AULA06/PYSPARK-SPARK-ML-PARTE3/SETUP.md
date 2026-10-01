# Setup — Spark ML, parte 3

Este laboratório local usa Spark MLlib do PySpark; não precisa de Docker nem de pacotes externos de machine learning.

## Requisitos

| Item | Versão |
| --- | --- |
| Python | 3.8 a 3.12; 3.11 recomendado |
| Java | JDK/JRE 8, 11 ou 17 |
| PySpark | 3.5.3 |
| Hadoop no Windows | `winutils.exe` e `hadoop.dll`, série 3.3.5 |

## Windows

```powershell
cd AULA06\PYSPARK-SPARK-ML-PARTE3
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
```

Copie `hadoop/` de uma aula já configurada ou baixe os binários:

```powershell
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll $R/hadoop.dll
```

Confira o ambiente:

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Complete o exercício de [EXERCICIOS_SPARK_ML_PARTE3.md](EXERCICIOS_SPARK_ML_PARTE3.md) antes de executar o pipeline Gold.

## Linux e macOS

```bash
cd AULA06/PYSPARK-SPARK-ML-PARTE3
python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3"
./.venv/bin/python verificar_ambiente.py
```

Depois de resolver o exercício:

```bash
./.venv/bin/python executar_pipeline.py
```

## Artefatos

`dados/` guarda a fonte sintética; `camadas/` guarda as tabelas Parquet, métricas e modelo. Ambos são ignorados pelo Git e podem ser removidos para começar de novo. `winutils.exe` e `hadoop.dll` só são necessários no Windows.
