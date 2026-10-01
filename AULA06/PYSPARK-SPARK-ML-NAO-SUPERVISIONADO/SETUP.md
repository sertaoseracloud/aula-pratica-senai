# Setup — aprendizado não supervisionado

## Requisitos

Python 3.8–3.12 (3.11 recomendado), Java 8/11/17 e `pyspark==3.5.3`. No Windows, use também `winutils.exe` e `hadoop.dll`; veja o [setup da AULA03](../../AULA03/PYSPARK-BASICO/SETUP.md).

## Instalação

```powershell
cd AULA06\PYSPARK-SPARK-ML-NAO-SUPERVISIONADO
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
```

Copie `hadoop/` de uma aula já configurada ou siga as instruções da AULA03. Verifique o ambiente:

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

Resolva os exercícios de [EXERCICIOS_NAO_SUPERVISIONADO.md](EXERCICIOS_NAO_SUPERVISIONADO.md) antes de executar `executar_pipeline.py`.

## Linux e macOS

```bash
cd AULA06/PYSPARK-SPARK-ML-NAO-SUPERVISIONADO
python3.11 -m venv .venv
./.venv/bin/python -m pip install "pyspark==3.5.3"
./.venv/bin/python verificar_ambiente.py
```

Depois dos exercícios, execute `./.venv/bin/python executar_pipeline.py`.
