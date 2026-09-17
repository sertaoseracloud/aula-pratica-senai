# Setup do laboratório — o que instalar e configurar

Guia de instalação deste laboratório. É a soma dos dois anteriores: o **Python/Java/PySpark** do laboratório 100% local ([`AULA05/PYSPARK-MEDALHAO`](../PYSPARK-MEDALHAO/SETUP.md)) mais o **floci-az** do laboratório de ETL na nuvem ([`AULA04/PYSPARK-AZURE-BLOB`](../../AULA04/PYSPARK-AZURE-BLOB/SETUP.md)). Se você já rodou os dois nesta máquina, só falta criar o venv desta pasta — pule para o Passo 3.

Tempo total: **~5 minutos** se você já tem Docker e Java.

## O que é preciso, e por quê

| Item | Versão | Por quê | Tamanho |
| --- | --- | --- | --- |
| **Docker Engine + Compose** | mesma das outras aulas | roda o floci-az localmente, sem conta do Azure | — |
| **floci-az** | `floci/floci-az:latest` | emulador de Azure (Blob Storage), compatível com o SDK oficial | imagem leve |
| **Python 3.8 a 3.12** | 3.11 recomendado | o PySpark 3.5 não suporta 3.13 nem 3.14 | — |
| **JDK 8, 11 ou 17** | 8 já basta | o Spark é escrito em Scala e roda numa JVM | ~200 MB |
| **PySpark 3.5.3** | exata | cada um dos três jobs sobe a própria sessão, sempre local | 590 MB com o venv |
| **`azure-storage-blob`** | SDK oficial da Microsoft | quem conversa com o Blob é o Python, não o Spark | ~5 MB |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows** | 196 KB |

## Passo 1 — floci-az

Crie `docker-compose.yml` nesta pasta (o `.gitignore` exclui esse arquivo — o conteúdo fica só aqui):

```yaml
services:
  floci-az:
    image: floci/floci-az:latest
    container_name: floci-az-medalhao
    ports:
      - "4577:4577"
```

```bash
docker compose up -d --wait
docker ps --filter name=floci-az-medalhao
```

> **Se você já tem o `floci-az` da AULA04 rodando na porta 4577, não precisa subir um segundo contêiner** — pode reaproveitar o mesmo, os dois laboratórios usam containers de Blob diferentes (`temperaturas-sc` vs. `medalhao-clima-sc`) dentro da mesma conta emulada. Só evite rodar `docker compose up` duas vezes apontando pra mesma porta a partir de pastas diferentes; se precisar de portas separadas, mude o mapeamento aqui e ajuste `BlobEndpoint` dentro de `CONNECTION_STRING_FLOCI_AZ` no [`comum.py`](comum.py).

## Passo 2 — Python e Java

```powershell
py -0
java -version
```

Precisa de Python entre **3.8 e 3.12** e qualquer JDK/JRE 8, 11 ou 17 no `PATH`.

## Passo 3 — O ambiente virtual, o PySpark e o SDK do Azure

```powershell
cd C:\repo\aula-pratica\AULA05\PYSPARK-MEDALHAO-AZURE-BLOB

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" azure-storage-blob
```

## Passo 4 — `winutils.exe` (só no Windows)

```powershell
New-Item -ItemType Directory -Force hadoop\bin | Out-Null
$R = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin"
curl.exe -sSL -o hadoop\bin\winutils.exe $R/winutils.exe
curl.exe -sSL -o hadoop\bin\hadoop.dll   $R/hadoop.dll
```

> Se você já rodou a AULA03 ou qualquer outro laboratório de PySpark nesta máquina, copie a pasta `hadoop/` de lá.

## Passo 5 — Conferir e rodar

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
.\.venv\Scripts\python.exe 00_conectar_blob.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_limpeza.py
.\.venv\Scripts\python.exe 03_gold_agregados.py
```

Ou, depois de completar os cinco exercícios, tudo de uma vez:

```powershell
.\.venv\Scripts\python.exe executar_pipeline.py
```

---

## Trocando o floci-az por uma conta Azure real

Mesma variável do laboratório de ETL da AULA04:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=SUACONTA;AccountKey=SUACHAVE;EndpointSuffix=core.windows.net"
```

Nenhum dos três jobs muda — todos leem `AZURE_CONNECTION_STRING` do [`comum.py`](comum.py).

---

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| porta `4577` já em uso | outro floci-az já está rodando (ex.: o da AULA04) | pare o outro contêiner, ou mude a porta e ajuste `comum.py` |
| `ModuleNotFoundError: No module named 'azure.storage.blob'` | pacote não instalado neste interpretador | `.\.venv\Scripts\python.exe -m pip install azure-storage-blob` |
| `ServiceRequestError` / `Connection refused` | o floci-az não está rodando | `docker compose up -d --wait` |
| `Bronze nao encontrada no Blob` / `Silver nao encontrada no Blob` | job anterior não rodou, ou rodou contra outro container/porta | rode os jobs na ordem, confira `AZURE_STORAGE_CONTAINER` |
| erros de `winutils.exe`, Java ou versão do PySpark | mesmas causas da AULA03 | veja a tabela do SETUP.md da AULA03 |

Erros que aparecem **durante** os jobs estão na tabela do [README](README.md#se-algo-der-errado).

---

## Desinstalar

```powershell
docker compose down -v
Remove-Item -Recurse -Force .venv, hadoop, dados, camadas
```
