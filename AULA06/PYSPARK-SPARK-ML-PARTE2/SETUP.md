# Setup — Spark ML, parte 2

Esta continuação usa o mesmo ambiente local da AULA06 parte 1. Spark MLlib acompanha o PySpark, portanto não instale bibliotecas de ML adicionais.

## Requisitos

| Item | Versão |
| --- | --- |
| Python | 3.8 a 3.12; 3.11 recomendado |
| Java | JDK/JRE 8, 11 ou 17 |
| PySpark | 3.5.3 |
| Hadoop no Windows | `winutils.exe` e `hadoop.dll` da série 3.3.5 |

## Windows

Na pasta do laboratório:

```powershell
cd AULA06\PYSPARK-SPARK-ML-PARTE2
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
```

Copie `hadoop/` de uma aula já configurada, ou baixe os binários:

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

Complete os TODOs descritos em [EXERCICIOS_SPARK_ML_PARTE2.md](EXERCICIOS_SPARK_ML_PARTE2.md) antes de executar `executar_pipeline.py`.

## Linux e macOS

```bash
cd AULA06/PYSPARK-SPARK-ML-PARTE2
python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3"
./.venv/bin/python verificar_ambiente.py
```

Depois de resolver os exercícios:

```bash
./.venv/bin/python executar_pipeline.py
```

## Observações

- `dados/` contém os CSVs sintéticos gerados pelo job Bronze.
- `camadas/` contém Parquet, previsões e modelos gravados pelo pipeline.
- Os dois diretórios são ignorados pelo Git e podem ser apagados para reiniciar o laboratório.
- `winutils.exe` e `hadoop.dll` são exigidos somente no Windows.
