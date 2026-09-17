# Setup do laboratório — o que instalar e configurar

Guia de instalação deste laboratório. É o mesmo ambiente **local** da AULA03 — sem Docker, sem conta de nuvem, sem SDK além do próprio PySpark. Se você já rodou a AULA03 nesta máquina, o Python, o Java e o `winutils.exe` já estão prontos: pule direto para o Passo 3.

Tempo total: **~3 minutos**, quase todo o pip baixando o PySpark.

## O que é preciso, e por quê

| Item | Versão | Por quê | Tamanho |
| --- | --- | --- | --- |
| **Python 3.8 a 3.12** | 3.11 recomendado | o PySpark 3.5 não suporta 3.13 nem 3.14 | — |
| **JDK 8, 11 ou 17** | 8 já basta | o Spark é escrito em Scala e roda numa JVM | ~200 MB |
| **PySpark 3.5.3** | exata | traz o Spark inteiro dentro do pacote Python | 590 MB com o venv |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows**: o Hadoop precisa deles até para gravar em disco local | 196 KB |

Nada de Docker, nada de SDK de nuvem. As três camadas (Bronze, Silver, Gold) são só pastas no seu disco.

## Passo 1 — Python

```powershell
py -0
```

Precisa de uma versão entre **3.8 e 3.12**. Veja o SETUP.md da AULA03 se precisar instalar.

## Passo 2 — Java

```bash
java -version
```

Qualquer JDK/JRE 8, 11 ou 17 no `PATH` resolve. Detalhes no SETUP.md da AULA03, Passo 2.

## Passo 3 — O ambiente virtual e o PySpark

```powershell
cd C:\repo\aula-pratica\AULA05\PYSPARK-MEDALHAO

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3"
```

No Linux ou macOS:

```bash
cd ~/repo/aula-pratica/AULA05/PYSPARK-MEDALHAO

python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3"
```

## Passo 4 — `winutils.exe` (só no Windows)

Pule este passo no Linux e no macOS. Se você já rodou a AULA03, o [`hadoop-3.3.5`](https://github.com/cdarlint/winutils) da AULA03, do laboratório do Blob ou do S3, copie a pasta `hadoop/` de lá — o conteúdo é idêntico.

```powershell
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll   $R/hadoop.dll
Get-ChildItem hadoop\bin | Select-Object Name, Length
```

```
Name         Length
----         ------
hadoop.dll    84992
winutils.exe 112640
```

## Passo 5 — Conferir

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

```
Verificando o ambiente em C:\repo\aula-pratica\AULA05\PYSPARK-MEDALHAO

[  OK   ] Python                 3.11.15  (...\.venv\Scripts\python.exe)
[  OK   ] PySpark                3.5.3
[  OK   ] Java                   java version "1.8.0_461"
[  OK   ] winutils (Windows)     hadoop/bin/ completo

Ambiente pronto. Comece por: python 01_bronze_ingestao.py
```

## Passo 6 — Rodar

Um job de cada vez, na ordem — cada um depende do anterior já ter gravado a sua camada:

```powershell
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_limpeza.py
.\.venv\Scripts\python.exe 03_gold_agregados.py
```

Ou, depois de completar os cinco exercícios, tudo de uma vez:

```powershell
.\.venv\Scripts\python.exe executar_pipeline.py
```

No estado em que vem no clone, `01_bronze_ingestao.py` para no primeiro `# TODO`. Veja o [EXERCICIOS_MEDALHAO.md](EXERCICIOS_MEDALHAO.md) para completar os cinco exercícios, um por vez.

---

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| `ModuleNotFoundError: No module named 'pyspark'` | está rodando o Python do sistema | use `.\.venv\Scripts\python.exe` |
| `Bronze nao encontrada em ...` ao rodar a Silver | você rodou a Silver antes da Bronze, ou apagou `camadas/` | rode `python 01_bronze_ingestao.py` primeiro |
| `Silver nao encontrada em ...` ao rodar a Gold | mesma causa, um job adiante | rode os jobs na ordem: Bronze, Silver, Gold |
| erro de gateway do py4j, `HADOOP_HOME` ausente, `UnsupportedClassVersionError` | Java ausente, `winutils.exe` faltando, ou PySpark 4.x com Java 8 | veja a tabela do SETUP.md da AULA03 |

Erros que aparecem **durante** os jobs (e não na instalação) estão na tabela do [README](README.md#se-algo-der-errado).

---

## Desinstalar

```powershell
Remove-Item -Recurse -Force .venv, hadoop, dados, camadas
```
