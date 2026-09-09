# Setup do laboratório — o que instalar e configurar

Guia de instalação da **AULA03**. Este laboratório **não usa Docker**: tudo roda no seu Python, e a saída vai para uma pasta no seu disco.

Tempo total: **~3 minutos**, quase todo o pip baixando o PySpark. Só é feito uma vez.

## O que é preciso, e por quê

| Item | Versão | Por quê | Tamanho |
| --- | --- | --- | --- |
| **Python 3.8 a 3.12** | 3.11 recomendado | o PySpark 3.5 não suporta 3.13 nem 3.14 | — |
| **JDK 8, 11 ou 17** | 8 já basta | o Spark é escrito em Scala e roda numa JVM | ~200 MB |
| **PySpark 3.5.3** | exata | traz o Spark inteiro dentro do pacote Python | 590 MB com o venv |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows**: o Hadoop precisa deles até para gravar em disco local | 196 KB |
| `pandas` + `pyarrow` | qualquer | **opcionais** — só a seção 6 do script 01 os usa | ~90 MB |
| `curl` | qualquer | baixar os dois binários do Hadoop (já vem no Windows 10+) | — |

Não é preciso instalar Spark, Hadoop, Scala, Java IDE, Jupyter nem Docker. O pacote `pyspark` do pip **é** o Spark.

---

## Passo 1 — Python

Confira quais versões você tem:

```powershell
py -0
```

```
 -V:3.14          Python 3.14 (64-bit)
 -V:Astral/CPython3.12.13 CPython 3.12.13 (64-bit)
 -V:Astral/CPython3.11.15 CPython 3.11.15 (64-bit)
```

Precisa de uma entre **3.8 e 3.12**. Se só houver 3.13/3.14, instale a 3.11 de [python.org/downloads](https://www.python.org/downloads/release/python-3119/) marcando *Add python.exe to PATH*, ou com `uv python install 3.11` se você usa o uv.

> **O nome do seletor depende de como o Python foi instalado.** Instalações vindas do uv/Astral aparecem como `Astral/CPython3.11.15`, e aí `py -3.11` pode não encontrá-las — use o rótulo completo que o `py -0` mostrou. É a diferença entre `py -3.11 -m venv .venv` e `py -V:Astral/CPython3.11.15 -m venv .venv`.

## Passo 2 — Java

```bash
java -version
```

```
java version "1.8.0_461"
```

Se aparecer uma versão, está resolvido — **JRE serve**, não precisa ser JDK completo para rodar os labs. Se der "comando não encontrado", instale o [Temurin 17](https://adoptium.net/temurin/releases/?version=17) (instalador `.msi` no Windows, marque a opção de adicionar ao `PATH`).

Não é necessário definir `JAVA_HOME`: o PySpark encontra o Java pelo `PATH`. Defina apenas se tiver mais de um JDK e quiser fixar qual será usado.

> **Cuidado com o PySpark 4.x.** Ele exige **JDK 17 ou superior** e não sobe em Java 8 — o erro fala de classe não encontrada, não de versão de Java. Este laboratório fixa `pyspark==3.5.3` justamente para rodar em Java 8. O `verificar_ambiente.py` detecta essa combinação e avisa.

## Passo 3 — O ambiente virtual e o PySpark

Tudo dentro da pasta do laboratório:

```powershell
cd C:\repo\aula-pratica\AULA03\PYSPARK-BASICO

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" pandas pyarrow
```

No Linux ou macOS:

```bash
cd ~/repo/aula-pratica/AULA03/PYSPARK-BASICO

python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3" pandas pyarrow
```

**Por que dentro da pasta:** os scripts esperam encontrar o interpretador em `.venv`, e o VS Code o oferece automaticamente ao abrir a pasta. O `.venv` está no `.gitignore` — são 590 MB de dependência, não código do laboratório.

**Por que a versão fixa:** `pyspark==3.5.3` roda em Java 8 e em Python 3.8–3.12. Um `pip install pyspark` sem versão traz o 4.x, que exige Java 17 e produz um erro que não menciona Java.

## Passo 4 — `winutils.exe` (só no Windows)

Pule este passo no Linux e no macOS.

Mesmo **sem S3, sem cluster e gravando só em disco local**, o Spark usa as bibliotecas de sistema de arquivos do Hadoop para qualquer escrita — e a camada que verifica permissões de arquivo no Windows é implementada em dois binários nativos. Sem eles a sessão nem chega a subir:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.
```

No PowerShell:

```powershell
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll   $R/hadoop.dll
Get-ChildItem hadoop\bin | Select-Object Name, Length
```

Saída esperada — **confira os tamanhos**:

```
Name         Length
----         ------
hadoop.dll    84992
winutils.exe 112640
```

> **Se vierem arquivos de ~470 bytes**, o CDN devolveu uma página de erro (`503 Backend.max_conn reached`) e o `curl` a salvou com nome de executável. Aconteceu durante a validação deste laboratório. Apague `hadoop\bin` e repita — o `verificar_ambiente.py` também detecta isso.

Em Git Bash ou WSL, a versão repetível (rodar duas vezes não baixa de novo):

```bash
mkdir -p hadoop/bin && cd hadoop/bin
R=https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin
[ -f winutils.exe ] || curl -sSLO $R/winutils.exe
[ -f hadoop.dll ]   || curl -sSLO $R/hadoop.dll
cd ../..
```

Você **não** precisa definir `HADOOP_HOME` na mão: o [`comum.py`](comum.py) aponta a variável para essa pasta a cada execução, e falha com instrução clara se os arquivos não estiverem lá.

## Passo 5 — Conferir

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

```
Verificando o ambiente em C:\repo\aula-pratica\AULA03\PYSPARK-BASICO

[  OK   ] Python                 3.11.15  (...\.venv\Scripts\python.exe)
[  OK   ] PySpark                3.5.3
[  OK   ] Java                   java version "1.8.0_461"
[  OK   ] winutils (Windows)     hadoop/bin/ completo
[  OK   ] pandas (opcional)      3.0.5

Ambiente pronto. Comece por: python 00_primeiro_contato.py
```

O script roda em ~1 segundo e **não sobe sessão Spark** — ele existe porque cada uma dessas quatro dependências falha de um jeito que não aponta para a causa:

| Falta | Mensagem que você receberia |
| --- | --- |
| interpretador errado | `ModuleNotFoundError: No module named 'pyspark'` |
| Python 3.13 ou 3.14 | a instalação funciona; a execução falha em outro ponto |
| Java | erro de gateway do py4j, sem citar Java |
| `winutils.exe` | `HADOOP_HOME and hadoop.home.dir are unset` |

Quando algo falta, ele diz o que fazer e sai com código 1:

```
[ FALTA ] Python                 3.14.4 -- o PySpark 3.5 suporta apenas 3.8 a 3.12
         -> recrie o ambiente: py -3.11 -m venv .venv
[ AVISO ] Ambiente virtual       voce NAO esta usando o .venv desta pasta
         -> rode com .\.venv\Scripts\python.exe verificar_ambiente.py

2 item(ns) a resolver: Python, Ambiente virtual
```

## Passo 6 — Rodar

```powershell
.\.venv\Scripts\python.exe 00_primeiro_contato.py
```

Se preferir não digitar o caminho do interpretador em cada comando, ative o ambiente uma vez por terminal:

```powershell
.\.venv\Scripts\Activate.ps1     # PowerShell
python 00_primeiro_contato.py
```

```bash
source .venv/bin/activate        # Linux, macOS, Git Bash
python 00_primeiro_contato.py
```

> No PowerShell, `Activate.ps1` pode ser bloqueado pela política de execução (`execution of scripts is disabled on this system`). Ou você libera com `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou simplesmente continua chamando `.\.venv\Scripts\python.exe` diretamente — que é o que o README faz, justamente para não depender disso.

Estas duas linhas aparecem no início de toda execução e **são normais**, não são erro:

```
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
```

A ordem sugerida é [`00`](00_primeiro_contato.py) → [`01`](01_dataframe.py) → [`02`](02_funcionalidades.py) → [`03`](03_etl_local.py), e depois os [exercícios](EXERCICIOS.md).

---

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| `py: command not found` | o *Python launcher* não foi instalado | use `python -m venv .venv` (com o Python correto no `PATH`) |
| `py -3.11` não encontra a versão | ela veio do uv/Astral | use o rótulo completo de `py -0`, ex. `py -V:Astral/CPython3.11.15` |
| `ModuleNotFoundError: No module named 'pyspark'` | está rodando o Python do sistema | use `.\.venv\Scripts\python.exe` |
| `Activate.ps1 cannot be loaded` | política de execução do PowerShell | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, ou chame o `python.exe` direto |
| pip lento ou travando | são 300 MB de PySpark | espere; `--no-cache-dir` se o disco estiver cheio |
| `winutils.exe` com 470 bytes | página de erro do CDN salva como executável | apague `hadoop\bin` e repita o Passo 4 |
| erro de gateway do py4j | não há Java no `PATH` | Passo 2 |
| `UnsupportedClassVersionError` | PySpark 4.x com Java 8 | fixe `pyspark==3.5.3` (Passo 3) |
| `Python worker failed to connect back` | worker subiu com outro Python | o `comum.py` já fixa `PYSPARK_PYTHON`; rode de novo |
| antivírus bloqueando `winutils.exe` | é um executável baixado da internet | libere a pasta `hadoop\bin` ou use o WSL |

Erros que aparecem **durante** os scripts (e não na instalação) estão na tabela do [README](README.md#se-algo-der-errado).

---

## Desinstalar

Tudo o que o laboratório instala está dentro da própria pasta — não há nada no registro, no `PATH` ou no seu Python global:

```powershell
Remove-Item -Recurse -Force .venv, hadoop, dados, saida
```

O Java, se você o instalou para este laboratório, sai pelo painel de programas do Windows.
