# Setup do laboratório — o que instalar e configurar

Guia de instalação da **AULA04**. É a AULA03 mais uma peça: um contêiner **floci-az** ([floci.io/az](https://floci.io/az/)), um emulador de Azure leve rodando em Docker. Tudo o resto — Python, Java, PySpark, `winutils.exe` — é idêntico ao que você já instalou (ou vai instalar) para a AULA03. Se você já tem aquele ambiente pronto, pule direto para o Passo 1.

Tempo total: **~5 minutos** se você já tem Docker e Java; a maior parte é o download do PySpark, igual à AULA03.

## O que é preciso, e por quê

| Item | Versão | Por quê | Tamanho |
| --- | --- | --- | --- |
| **Docker Engine + Compose** | mesma da AULA01/02 | roda o floci-az localmente, sem conta do Azure | — |
| **floci-az** | `floci/floci-az:latest` | emulador de Azure (Blob Storage entre outros serviços), compatível com o SDK oficial | imagem leve — sobe em ~24 ms, ~13 MiB em repouso |
| **Python 3.8 a 3.12** | 3.11 recomendado | o PySpark 3.5 não suporta 3.13 nem 3.14 | — |
| **JDK 8, 11 ou 17** | 8 já basta | o Spark é escrito em Scala e roda numa JVM | ~200 MB |
| **PySpark 3.5.3** | exata | o mesmo motor da AULA03 — aqui ele só lê e grava arquivo local, nunca fala com o Blob diretamente | 590 MB com o venv |
| **`azure-storage-blob`** | SDK oficial da Microsoft | quem conversa com o Blob (floci-az ou Azure real) é o Python, não o Spark | ~5 MB |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows**: mesma exigência da AULA03 | 196 KB |

Você **não** precisa de conta no Azure, cartão de crédito, nem `az cli`. O floci-az implementa a mesma API REST do Blob Storage real e usa a mesma conta de desenvolvimento padrão (`devstoreaccount1`) que o Azurite — o SDK que fala com um fala com o outro, só muda a `connection string`.

---

## Passo 1 — floci-az (o Blob Storage local)

Crie `docker-compose.yml` nesta pasta (o `.gitignore` do repositório exclui esse arquivo — o conteúdo fica só aqui):

```yaml
services:
  floci-az:
    image: floci/floci-az:latest
    container_name: floci-az-lab
    ports:
      - "4577:4577"        # REST -- Blob, Tabelas e o resto dos servicos emulados
```

Suba o contêiner:

```bash
docker compose up -d --wait
```

`--wait` aqui garante só que o processo respondeu — o mesmo aviso que vale desde a AULA01 ("`--wait` não significa que está pronto"). A confirmação de verdade vem do Passo 6 (`verificar_ambiente.py`), que tenta abrir a porta.

Confira que o contêiner subiu:

```bash
docker ps --filter name=floci-az-lab
```

```
CONTAINER ID   IMAGE                  STATUS
6b2f9a1c3d4e   floci/floci-az:latest  Up 5 seconds
```

## Passo 2 — Python

Igual à AULA03:

```powershell
py -0
```

Precisa de uma versão entre **3.8 e 3.12**. Veja o SETUP.md da AULA03 se precisar instalar.

## Passo 3 — Java

```bash
java -version
```

Qualquer JDK/JRE 8, 11 ou 17 no `PATH` resolve. Detalhes no SETUP.md da AULA03, Passo 2.

## Passo 4 — O ambiente virtual, o PySpark e o SDK do Azure

```powershell
cd C:\repo\aula-pratica\AULA04\PYSPARK-AZURE-BLOB

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" azure-storage-blob
```

No Linux ou macOS:

```bash
cd ~/repo/aula-pratica/AULA04/PYSPARK-AZURE-BLOB

python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3" azure-storage-blob
```

A única diferença em relação à AULA03 é o pacote `azure-storage-blob` — o SDK oficial que fala com o Blob (floci-az ou Azure real). `pandas`/`pyarrow` não são usados aqui.

## Passo 5 — `winutils.exe` (só no Windows)

Idêntico ao Passo 4 do SETUP.md da AULA03 — o Spark continua lendo e gravando em disco local (o Blob entra e sai só via SDK, nunca via `spark.read`/`spark.write` diretamente):

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

> Se você já rodou a AULA03 na mesma máquina, pode copiar a pasta `hadoop/` de lá em vez de baixar de novo — o conteúdo é idêntico.

## Passo 6 — Conferir

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

```
Verificando o ambiente em C:\repo\aula-pratica\AULA04\PYSPARK-AZURE-BLOB

[  OK   ] Python                 3.11.15  (...\.venv\Scripts\python.exe)
[  OK   ] PySpark                3.5.3
[  OK   ] Java                   java version "1.8.0_461"
[  OK   ] winutils (Windows)     hadoop/bin/ completo
[  OK   ] azure-storage-blob     12.24.0
[  OK   ] floci-az               respondendo em 127.0.0.1:4577

Ambiente pronto. Comece por: python 00_conectar_blob.py
```

Se o emulador não estiver rodando, a última linha vira:

```
[ FALTA ] floci-az               sem resposta em 127.0.0.1:4577
         -> suba o container: docker compose up -d --wait (veja o SETUP.md)
```

## Passo 7 — Rodar

```powershell
.\.venv\Scripts\python.exe 00_conectar_blob.py
.\.venv\Scripts\python.exe 01_etl_temperaturas_sc.py
```

A ordem é essa: primeiro o [`00_conectar_blob.py`](00_conectar_blob.py) prova que a conexão com o Blob funciona sozinha (sem Spark no meio), depois o [`01_etl_temperaturas_sc.py`](01_etl_temperaturas_sc.py) faz o ETL inteiro. Detalhes de cada um no [README](README.md).

---

## Trocando o floci-az por uma conta Azure real

Nenhum script muda. Exporte a variável de ambiente antes de rodar:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=SUACONTA;AccountKey=SUACHAVE;EndpointSuffix=core.windows.net"
```

No PowerShell:

```powershell
$env:AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=SUACONTA;AccountKey=SUACHAVE;EndpointSuffix=core.windows.net"
```

O [`comum.py`](comum.py) lê essa variável e, se ela existir, ignora a connection string do floci-az. **Nunca coloque uma connection string de conta real dentro do código** — é exatamente esse acoplamento que a variável de ambiente evita.

---

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| `docker: 'compose' is not a docker command` | Docker Compose não instalado ou versão antiga (v1) | instale o plugin `docker-compose-plugin`, ou use `docker-compose` (com hífen) |
| porta `4577` já em uso | outro processo (outro emulador, outro serviço local) já usa a porta | mude o mapeamento no `docker-compose.yml` (`"4578:4577"`) e ajuste `BlobEndpoint` no `comum.py` |
| `ModuleNotFoundError: No module named 'azure.storage.blob'` | pacote não instalado neste interpretador | `.\.venv\Scripts\python.exe -m pip install azure-storage-blob` |
| `ServiceRequestError` / `Connection refused` ao rodar `00_conectar_blob.py` | o floci-az não está rodando | `docker compose up -d --wait`, depois confira com `docker ps` |
| `azure.core.exceptions.ResourceExistsError` ao criar o container | corrida entre duas execuções simultâneas, ou container já existe | normal na segunda execução se você tirou o `if not container.exists()`; o `comum.py` já trata isso |
| erros de `winutils.exe` ou versão de Java/Python | mesmas causas da AULA03 | veja a tabela do SETUP.md da AULA03 |

Erros que aparecem **durante** os scripts (e não na instalação) estão na tabela do [README](README.md#se-algo-der-errado).

---

## Desinstalar

```powershell
docker compose down -v
Remove-Item -Recurse -Force .venv, hadoop, dados, saida
```
