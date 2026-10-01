# Setup do laboratório Spark ML

O laboratório é local, sem Docker, nuvem ou biblioteca extra além do PySpark. O ambiente da AULA03 pode ser reutilizado: Spark MLlib é distribuído junto com `pyspark`.

## Requisitos

| Item | Versão | Motivo |
| --- | --- | --- |
| Python | 3.8 a 3.12; 3.11 recomendado | compatibilidade com PySpark 3.5.3 |
| Java | JDK/JRE 8, 11 ou 17 | runtime JVM do Spark |
| PySpark | 3.5.3 | inclui Spark SQL e Spark MLlib |
| Hadoop Windows | winutils 3.3.5 | necessário no Windows para operações locais do Spark |

## Instalar no Windows

Abra PowerShell na pasta do laboratório:

```powershell
cd AULA06\PYSPARK-SPARK-ML
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
```

Se já tem a pasta `hadoop/` de uma aula anterior, copie-a para esta pasta. Caso contrário, baixe os binários do projeto [winutils](https://github.com/cdarlint/winutils):

```powershell
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll $R/hadoop.dll
```

Confira o ambiente e execute:

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

As funções de modelo da Gold começam como exercícios `TODO`. Complete os três
conforme [EXERCICIOS_SPARK_ML.md](EXERCICIOS_SPARK_ML.md) antes de executar
`executar_pipeline.py`.

## Linux e macOS

```bash
cd AULA06/PYSPARK-SPARK-ML
python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3"
./.venv/bin/python verificar_ambiente.py
```

Complete os três `TODO`s da Gold seguindo [EXERCICIOS_SPARK_ML.md](EXERCICIOS_SPARK_ML.md)
antes de executar `./.venv/bin/python executar_pipeline.py`.

`winutils.exe` e `hadoop.dll` são necessários somente no Windows.

## Erros comuns

| Sintoma | Causa provável | Solução |
| --- | --- | --- |
| `No module named pyspark` | Python diferente do ambiente virtual | execute com `\.venv\Scripts\python.exe` no Windows ou `./.venv/bin/python` nos demais |
| erro de gateway do Py4J | Java ausente ou incompatível | confira `java -version` e instale Java 8, 11 ou 17 |
| `winutils.exe` ausente | pasta Hadoop não copiada no Windows | copie `hadoop/` de uma aula anterior ou baixe os binários acima |
| Silver ou Gold ausente | jobs executados fora de ordem | execute Bronze, Silver e Gold nessa ordem |
| erro em coluna ou tipo no modelo | alterou features ou fonte sem ajustar o pipeline | confirme nomes/tipos em Silver e sincronize-os em `inputCols` e `labelCol` |
