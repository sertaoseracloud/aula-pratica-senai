# Setup do laboratório — o que instalar e configurar

Guia de instalação deste laboratório. É o mesmo ambiente da pasta [`PYSPARK-AZURE-BLOB`](../PYSPARK-AZURE-BLOB/SETUP.md) ao lado, trocando o emulador de nuvem: aqui é o **[floci](https://floci.io/aws/)**, um emulador de AWS em Docker. Se você já rodou o laboratório do Blob nesta máquina, o Python, o Java e o `winutils.exe` já estão prontos — pule direto para o Passo 1.

Tempo total: **~5 minutos** se você já tem Docker e Java.

## O que é preciso, e por quê

| Item | Versão | Por quê | Tamanho |
| --- | --- | --- | --- |
| **Docker Engine + Compose** | mesma das outras aulas | roda o floci localmente, sem conta AWS | — |
| **floci** | `floci/floci:latest` | emulador de AWS (S3 entre outros serviços), compatível com o boto3 | imagem leve |
| **Python 3.8 a 3.12** | 3.11 recomendado | o PySpark 3.5 não suporta 3.13 nem 3.14 | — |
| **JDK 8, 11 ou 17** | 8 já basta | o Spark é escrito em Scala e roda numa JVM | ~200 MB |
| **PySpark 3.5.3** | exata | o mesmo motor das outras aulas — aqui ele só lê e grava arquivo local, nunca fala com o S3 diretamente | 590 MB com o venv |
| **`boto3`** | SDK oficial da AWS | quem conversa com o S3 (floci ou AWS real) é o Python, não o Spark | ~15 MB |
| **`winutils.exe` + `hadoop.dll`** | série 3.3.5 | **só no Windows**: mesma exigência das outras aulas de PySpark | 196 KB |

Você **não** precisa de conta na AWS, cartão de crédito, nem `aws cli`. O floci implementa a mesma API REST do S3 real — o SDK que fala com um fala com o outro, só muda o `endpoint_url`.

---

## Passo 1 — floci (o S3 local)

Crie `docker-compose.yml` nesta pasta (o `.gitignore` do repositório exclui esse arquivo — o conteúdo fica só aqui):

```yaml
services:
  floci:
    image: floci/floci:latest
    container_name: floci-aws-lab
    ports:
      - "4566:4566"                              # REST -- S3 e o resto dos servicos emulados
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock # o floci usa isso para emular alguns servicos
```

> **Sobre o volume do Docker socket:** essa linha dá ao contêiner acesso ao daemon Docker do seu host — é o que o próprio projeto pede na documentação, para que o floci consiga subir contêineres auxiliares por trás de certos serviços emulados (não o S3 em si). Isso é um privilégio real, não cosmético: qualquer processo com esse socket pode iniciar outros contêineres na sua máquina. Adequado para um laboratório local; **não** é algo para replicar num ambiente compartilhado ou de produção sem entender essa implicação.

No Linux/macOS/WSL o caminho acima funciona direto. No Windows com Docker Desktop, o mesmo `docker-compose.yml` funciona sem alteração — o Docker Desktop expõe `/var/run/docker.sock` para os contêineres Linux mesmo rodando no Windows.

Suba o contêiner:

```bash
docker compose up -d --wait
```

Confira que ele subiu:

```bash
docker ps --filter name=floci-aws-lab
```

```
CONTAINER ID   IMAGE                STATUS
7c3f0a2b1d5e   floci/floci:latest   Up 5 seconds
```

## Passo 2 — Python e Java

Iguais às outras aulas de PySpark:

```powershell
py -0
java -version
```

Precisa de Python entre **3.8 e 3.12** e qualquer JDK/JRE 8, 11 ou 17 no `PATH`. Veja o SETUP.md da AULA03 se precisar instalar.

## Passo 3 — O ambiente virtual, o PySpark e o boto3

```powershell
cd C:\repo\aula-pratica\AULA04\PYSPARK-AWS-S3

py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install "pyspark==3.5.3" boto3
```

No Linux ou macOS:

```bash
cd ~/repo/aula-pratica/AULA04/PYSPARK-AWS-S3

python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install "pyspark==3.5.3" boto3
```

## Passo 4 — `winutils.exe` (só no Windows)

Idêntico às outras aulas de PySpark:

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

> Se você já rodou a AULA03 ou o laboratório do Blob nesta máquina, copie a pasta `hadoop/` de lá — o conteúdo é idêntico.

## Passo 5 — Conferir

```powershell
.\.venv\Scripts\python.exe verificar_ambiente.py
```

```
Verificando o ambiente em C:\repo\aula-pratica\AULA04\PYSPARK-AWS-S3

[  OK   ] Python                 3.11.15  (...\.venv\Scripts\python.exe)
[  OK   ] PySpark                3.5.3
[  OK   ] Java                   java version "1.8.0_461"
[  OK   ] winutils (Windows)     hadoop/bin/ completo
[  OK   ] boto3                  1.35.36
[  OK   ] floci                  respondendo em 127.0.0.1:4566

Ambiente pronto. Comece por: python 00_conectar_s3.py
```

Se o emulador não estiver rodando, a última linha vira:

```
[ FALTA ] floci                  sem resposta em 127.0.0.1:4566
         -> suba o container: docker compose up -d --wait (veja o SETUP.md)
```

## Passo 6 — Rodar

```powershell
.\.venv\Scripts\python.exe 00_conectar_s3.py
.\.venv\Scripts\python.exe 01_etl_energia_sc.py
```

A ordem é essa: primeiro o [`00_conectar_s3.py`](00_conectar_s3.py) prova que a conexão com o S3 funciona sozinha, depois o [`01_etl_energia_sc.py`](01_etl_energia_sc.py) — que é o **exercício**: veja o [EXERCICIOS_AWS.md](EXERCICIOS_AWS.md) para completá-lo.

---

## Trocando o floci por uma conta AWS real

Nenhum script muda. Antes de rodar:

```bash
export AWS_ENDPOINT_URL=""
export AWS_ACCESS_KEY_ID="SEU_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="SUA_SECRET_KEY"
export AWS_DEFAULT_REGION="sa-east-1"
```

No PowerShell:

```powershell
$env:AWS_ENDPOINT_URL = ""
$env:AWS_ACCESS_KEY_ID = "SEU_ACCESS_KEY"
$env:AWS_SECRET_ACCESS_KEY = "SUA_SECRET_KEY"
$env:AWS_DEFAULT_REGION = "sa-east-1"
```

`AWS_ENDPOINT_URL=""` (vazia, não ausente) é o que faz o [`comum.py`](comum.py) parar de apontar para o floci e deixar o boto3 resolver o endpoint padrão da AWS sozinho. **Nunca coloque uma credencial real dentro do código** — é exatamente esse acoplamento que a variável de ambiente evita.

---

## Problemas de instalação

| O que você vê | Causa | Solução |
| --- | --- | --- |
| `docker: 'compose' is not a docker command` | Docker Compose não instalado ou versão antiga (v1) | instale o plugin `docker-compose-plugin`, ou use `docker-compose` (com hífen) |
| porta `4566` já em uso | outro processo (LocalStack, outro floci) já usa a porta | mude o mapeamento no `docker-compose.yml` (`"4567:4566"`) e ajuste `AWS_ENDPOINT_URL` no `comum.py` |
| `ModuleNotFoundError: No module named 'boto3'` | pacote não instalado neste interpretador | `.\.venv\Scripts\python.exe -m pip install boto3` |
| `EndpointConnectionError` ao rodar `00_conectar_s3.py` | o floci não está rodando | `docker compose up -d --wait`, depois confira com `docker ps` |
| `InvalidLocationConstraint` ao criar o bucket | região diferente de `us-east-1` mandando `LocationConstraint` sem precisar (ou vice-versa) | o `comum.py` já trata isso; não mude `AWS_DEFAULT_REGION` sem revisar `obter_bucket()` |
| erros de `winutils.exe` ou versão de Java/Python | mesmas causas da AULA03 | veja a tabela do SETUP.md da AULA03 |

Erros que aparecem **durante** os scripts (e não na instalação) estão na tabela do [README](README.md#se-algo-der-errado).

---

## Desinstalar

```powershell
docker compose down -v
Remove-Item -Recurse -Force .venv, hadoop, dados, saida
```
