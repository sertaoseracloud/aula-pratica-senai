# Setup do laboratório — o que instalar e configurar

Guia de instalação deste laboratório. É a soma dos dois anteriores: o **Python/Java/PySpark** do laboratório 100% local ([`AULA05/PYSPARK-MEDALHAO`](../PYSPARK-MEDALHAO/SETUP.md)) mais o **floci** do laboratório de ETL na nuvem ([`AULA04/PYSPARK-AWS-S3`](../../AULA04/PYSPARK-AWS-S3/SETUP.md)). Se você já rodou os dois nesta máquina, só falta criar o venv desta pasta — pule para o Passo 2.

Tempo total: **~5 minutos** se você já tem Docker e Java.

## O que é preciso, e por quê

| Item | Versão | Por quê | Tamanho |
| --- | --- | --- | --- |
| **Docker Engine + Compose** | mesma das outras aulas | roda o floci localmente, sem conta AWS | — |
| **floci** | `floci/floci:latest` | emulador de AWS (S3), compatível com o boto3 | imagem leve |
| **Python 3.8 a 3.12** | 3.11 recomendado | o PySpark 3.5 não suporta 3.13 nem 3.14 | — |
| **JDK 8, 11 ou 17** | 8 já basta | o Spark é escrito em Scala e roda numa JVM | ~200 MB |
| **PySpark 3.5.3** | exata | cada um dos três jobs sobe a própria sessão, sempre local | 590 MB com o venv |
| **`boto3`** | SDK oficial da AWS | quem conversa com o S3 é o Python, não o Spark | ~15 MB |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows** | 196 KB |

## Passo 1 — floci

Crie `docker-compose.yml` nesta pasta (o `.gitignore` exclui esse arquivo — o conteúdo fica só aqui):

```yaml
services:
  floci:
    image: floci/floci:latest
    container_name: floci-aws-medalhao
    ports:
      - "4566:4566"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

> A linha do `docker.sock` dá ao contêiner acesso ao daemon Docker do seu host — é o que o próprio projeto pede na documentação. Veja o aviso completo no [SETUP.md do laboratório de ETL com S3](../../AULA04/PYSPARK-AWS-S3/SETUP.md#passo-1--floci-o-s3-local) da AULA04.

```bash
docker compose up -d --wait
docker ps --filter name=floci-aws-medalhao
```

> Se você já tem o `floci` da AULA04 rodando na porta 4566, **não suba um segundo contêiner** — pode reaproveitar o mesmo; os dois laboratórios usam buckets diferentes (`energia-sc` vs. `medalhao-residuos-sc`) dentro do mesmo emulador.

## Passo 2 — Python e Java

```powershell
py -0
java -version
```

## Passo 3 — O ambiente virtual, o PySpark e o boto3

```powershell
cd C:\repo\aula-pratica\AULA05\PYSPARK-MEDALHAO-AWS-S3

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" boto3
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
.\.venv\Scripts\python.exe 00_conectar_s3.py
.\.venv\Scripts\python.exe 01_bronze_ingestao.py
.\.venv\Scripts\python.exe 02_silver_limpeza.py
.\.venv\Scripts\python.exe 03_gold_agregados.py
```

Ou, depois de completar os cinco exercícios: `python executar_pipeline.py`.

---

## Trocando o floci por uma conta AWS real

Mesma variável do laboratório de ETL da AULA04:

```bash
export AWS_ENDPOINT_URL=""
export AWS_ACCESS_KEY_ID="SEU_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="SUA_SECRET_KEY"
export AWS_DEFAULT_REGION="sa-east-1"
```

Nenhum dos três jobs muda — todos leem essas variáveis do [`comum.py`](comum.py).

---

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| porta `4566` já em uso | outro floci já está rodando (ex.: o da AULA04) | pare o outro contêiner, ou mude a porta e ajuste `FLOCI_ENDPOINT` no `comum.py` |
| `ModuleNotFoundError: No module named 'boto3'` | pacote não instalado neste interpretador | `.\.venv\Scripts\python.exe -m pip install boto3` |
| `EndpointConnectionError` | o floci não está rodando | `docker compose up -d --wait` |
| `Bronze nao encontrada no S3` / `Silver nao encontrada no S3` | job anterior não rodou, ou rodou contra outro bucket | rode os jobs na ordem, confira `AWS_S3_BUCKET` |
| erros de `winutils.exe`, Java ou versão do PySpark | mesmas causas da AULA03 | veja a tabela do SETUP.md da AULA03 |

Erros que aparecem **durante** os jobs estão na tabela do [README](README.md#se-algo-der-errado).

---

## Desinstalar

```powershell
docker compose down -v
Remove-Item -Recurse -Force .venv, hadoop, dados, camadas
```
