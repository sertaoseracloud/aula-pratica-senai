# Setup — aprendizado supervisionado

## Requisitos

Python 3.8–3.12 (3.11 recomendado), Java 8/11/17 e `pyspark==3.5.3`. No Windows, instale também `winutils.exe` e `hadoop.dll`; veja o procedimento no [setup da AULA03](../../AULA03/PYSPARK-BASICO/SETUP.md).

## Instalação

```powershell
cd AULA06\PYSPARK-SPARK-ML-SUPERVISIONADO
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
```

Copie a pasta `hadoop/` de uma aula anterior ou siga as instruções da AULA03. Confira o ambiente e rode:

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe executar_pipeline.py
```

O Gold contém `TODO`s até os exercícios em [EXERCICIOS_SUPERVISIONADO.md](EXERCICIOS_SUPERVISIONADO.md) serem resolvidos.

## Linux e macOS

```bash
cd AULA06/PYSPARK-SPARK-ML-SUPERVISIONADO
python3.11 -m venv .venv
./.venv/bin/python -m pip install "pyspark==3.5.3"
./.venv/bin/python verificar_ambiente.py
```

Depois de completar os exercícios, execute `./.venv/bin/python executar_pipeline.py`.
