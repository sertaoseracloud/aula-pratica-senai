"""Exercicio de ETL -- temperaturas de um trimestre em dez cidades de SC,
lidas e gravadas no Azure Blob Storage.

    python 01_etl_temperaturas_sc.py

Este arquivo E o exercicio: ele te da a Fonte (gerador do CSV e do envio ao
Blob), o Extract, o Load e a Verificacao prontos, mas deixa quatro blocos de
Transform com `# TODO Exercicio N` no lugar do codigo. Leia o enunciado de
cada um no EXERCICIOS_AZURE.md, escreva a linha que falta e rode de novo.

Enquanto um TODO nao for preenchido, a funcao devolve `None` e o script para
com um erro claro (`AttributeError: 'NoneType' object has no attribute ...`)
apontando exatamente qual bloco falta -- o mesmo mecanismo do exercicio de
ETL da AULA03 (`05_etl_enem_sc.py`).

O padrao geral -- baixar do Blob, processar local, subir o resultado de
volta -- ja esta pronto nas etapas de Extract e Load. O que falta escrever
e a parte que nao muda de nuvem para nuvem: normalizar, validar, enriquecer
e resumir um DataFrame comum.

    dados/temperaturas_sc.csv                entrada local (gerada e enviada uma vez)
    dados/municipios_sc.csv                  tabela de apoio (municipio -> regiao)
    floci-az bruto/temperaturas_sc.csv       a mesma entrada, no Blob
    floci-az referencia/municipios_sc.csv    a mesma tabela, no Blob
    saida/temperaturas/municipio=.../        fato limpo, local
    floci-az processado/temperaturas/...     o mesmo fato, subido de volta
    floci-az processado/resumo_regiao/...    media trimestral por regiao
    floci-az processado/temp_rejeitadas/...  linhas que nao passaram na validacao
"""

import csv
import datetime
import random
import shutil

from comum import (
    DADOS,
    SAIDA,
    baixar_arquivo,
    criar_sessao,
    obter_container_client,
    subir_arquivo,
    subir_pasta,
    titulo,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ARQUIVO_TEMPERATURAS = DADOS / "temperaturas_sc.csv"
ARQUIVO_MUNICIPIOS = DADOS / "municipios_sc.csv"

# municipio -> (regiao, temperatura media base do trimestre, em graus C)
MUNICIPIOS_REGIOES = {
    "Florianopolis": ("Grande Florianopolis", 18.0),
    "Sao Jose": ("Grande Florianopolis", 18.0),
    "Joinville": ("Norte Catarinense", 18.0),
    "Jaragua do Sul": ("Norte Catarinense", 17.0),
    "Blumenau": ("Vale do Itajai", 17.0),
    "Itajai": ("Vale do Itajai", 18.0),
    "Balneario Camboriu": ("Vale do Itajai", 19.0),
    "Chapeco": ("Oeste Catarinense", 16.0),
    "Criciuma": ("Sul Catarinense", 17.0),
    "Lages": ("Serrana", 13.0),
}
NOMES_MUNICIPIOS = list(MUNICIPIOS_REGIOES)

DATA_INICIO = datetime.date(2024, 7, 1)
DATA_FIM = datetime.date(2024, 9, 30)          # trimestre: 92 dias, jul-set (inverno -> primavera)

SCHEMA_TEMPERATURAS = StructType(
    [
        StructField("estacao_id", IntegerType(), True),
        StructField("data", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("temperatura_min", DoubleType(), True),
        StructField("temperatura_max", DoubleType(), True),
        StructField("temperatura_media", DoubleType(), True),
        StructField("umidade_pct", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- dado, sem exercicio: gera os dois CSVs localmente e semeia o Blob
# ---------------------------------------------------------------------------
def gerar_csv_temperaturas() -> None:
    if ARQUIVO_TEMPERATURAS.exists():
        print(f"entrada ja existe: {ARQUIVO_TEMPERATURAS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(7)               # semente fixa: numeros reproduziveis
    dias = (DATA_FIM - DATA_INICIO).days + 1    # 92

    cabecalho = [
        "estacao_id", "data", "municipio", "temperatura_min",
        "temperatura_max", "temperatura_media", "umidade_pct",
    ]
    linhas = []
    estacao_id = 0
    for dia_idx in range(dias):
        data = DATA_INICIO + datetime.timedelta(days=dia_idx)
        # tendencia sazonal: sobe ~3 graus do inicio (inverno) ao fim (primavera)
        tendencia = 3.0 * (dia_idx / (dias - 1))
        for municipio in NOMES_MUNICIPIOS:
            estacao_id += 1
            _, base = MUNICIPIOS_REGIOES[municipio]
            media = base + tendencia + aleatorio.gauss(0, 2.5)
            variacao = aleatorio.uniform(3, 8)
            temp_min = round(media - variacao, 1)
            temp_max = round(media + variacao, 1)
            temp_media = round((temp_min + temp_max) / 2, 1)
            umidade = round(max(0.0, min(100.0, aleatorio.gauss(78, 10))), 1)

            linha = [estacao_id, data.isoformat(), municipio, temp_min, temp_max, temp_media, umidade]

            # ~5% de sujeira, dos tipos que um sensor de campo produz de verdade
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[2] = ""                            # municipio ausente
            elif sorteio < 0.02:
                linha[2] = linha[2].lower()               # municipio em formato errado
            elif sorteio < 0.03:
                linha[3], linha[4] = linha[4], linha[3]   # min/max trocados (sensor invertido)
            elif sorteio < 0.04:
                linha[4] = 65.0 if aleatorio.random() < 0.5 else -25.0   # fora da faixa fisica
            elif sorteio < 0.05:
                linha[6] = 130.0 if aleatorio.random() < 0.5 else -10.0  # umidade fora de 0-100

            linhas.append(linha)
            if sorteio < 0.004:
                linhas.append(list(linha))                # duplicata exata

    with ARQUIVO_TEMPERATURAS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    tamanho = ARQUIVO_TEMPERATURAS.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_TEMPERATURAS} ({len(linhas)} linhas, {tamanho:.0f} KB)")


def gerar_csv_municipios() -> None:
    if ARQUIVO_MUNICIPIOS.exists():
        print(f"entrada ja existe: {ARQUIVO_MUNICIPIOS} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    with ARQUIVO_MUNICIPIOS.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(["municipio", "regiao"])
        for municipio, (regiao, _) in MUNICIPIOS_REGIOES.items():
            escritor.writerow([municipio.upper(), regiao])

    print(f"entrada gerada: {ARQUIVO_MUNICIPIOS}")


def semear_blob(container) -> None:
    """Sobe as duas entradas para o Blob, se ainda nao estiverem la.

    Em produção alguem (ou algum outro pipeline) ja teria colocado o dado no
    Blob antes do seu job rodar. Aqui essa etapa e simulada uma unica vez --
    depois disso, o restante do script trata o Blob como a fonte de
    verdade, sem olhar para o arquivo local de novo.
    """
    for local, remoto in (
        (ARQUIVO_TEMPERATURAS, "bruto/temperaturas_sc.csv"),
        (ARQUIVO_MUNICIPIOS, "referencia/municipios_sc.csv"),
    ):
        if container.get_blob_client(remoto).exists():
            print(f"ja esta no Blob: {remoto} (nao reenviado)")
            continue
        subir_arquivo(container, local, remoto)
        print(f"enviado ao Blob: {local.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar do Blob, so entao ler com o Spark
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, container) -> tuple[DataFrame, DataFrame]:
    # O Spark local nao fala com o Blob Storage diretamente -- ele le do
    # disco. Por isso o download vem antes do `spark.read`, nao dentro dele.
    baixar_arquivo(container, "bruto/temperaturas_sc.csv", DADOS / "_baixado_temperaturas.csv")
    baixar_arquivo(container, "referencia/municipios_sc.csv", DADOS / "_baixado_municipios.csv")

    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_TEMPERATURAS)
        .csv(str(DADOS / "_baixado_temperaturas.csv"))
    )
    municipios = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(DADOS / "_baixado_municipios.csv"))
    )
    print("linhas baixadas do Blob (temperaturas):", bruto.count())
    print("linhas baixadas do Blob (municipios) ..:", municipios.count())
    bruto.show(5, truncate=False)
    return bruto, municipios


# ---------------------------------------------------------------------------
# T de Transform -- quatro blocos, quatro exercicios
# ---------------------------------------------------------------------------
def normalizar(bruto: DataFrame) -> DataFrame:
    """Exercicio 1 -- padronizar municipio, converter a data e tirar duplicatas.

    Tres coisas, nesta ordem:
    1. `municipio` para maiusculo e sem espaco nas pontas (F.upper(F.trim(...))
       -- mesmo padrao do exercicio de ENEM da AULA03).
    2. `data` de string para date, com F.to_date("data", "yyyy-MM-dd").
    3. dropDuplicates em ["estacao_id", "data", "municipio"] -- identifica
       uma leitura unica.

    Resposta esperada: 920 linhas (4 duplicatas removidas de 924 lidas).
    """
    # TODO Exercicio 1: normalizar municipio, converter a data e remover duplicatas.
    return None


def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Exercicio 2 -- separar leituras validas de invalidas, com o motivo.

    Uma leitura e valida quando, ao mesmo tempo:
    - `municipio` esta em NOMES_MUNICIPIOS (compare em maiusculo);
    - `temperatura_min` <= `temperatura_max`;
    - `temperatura_min` e `temperatura_max` estao dentro de [-15, 50];
    - `umidade_pct` esta dentro de [0, 100].

    Junte as quatro condicoes com `&` dentro de um F.coalesce(..., F.lit(False))
    -- exatamente como a `regra_valida` do 03_etl_local.py da AULA03. Sem o
    coalesce, uma condicao que vira NULL faz a linha inteira sumir dos dois
    lados (aprovadas e rejeitadas) sem que a conciliacao note.

    Monte a coluna `motivo` nos rejeitados com uma cadeia de F.when, nesta
    ordem de prioridade: municipio invalido, depois min > max, depois fora
    da faixa fisica, e o que sobrar (`.otherwise(...)`) e umidade invalida.
    Duas condicoes podem bater na mesma linha ao mesmo tempo -- a ordem
    decide qual motivo fica registrado.

    Resposta esperada: 886 aprovadas, 34 rejeitadas.

        temperatura_min maior que temperatura_max      11
        municipio invalida ou ausente                   8
        umidade fora da faixa (0 a 100)                 8
        temperatura fora da faixa fisica (-15 a 50)     7
    """
    nomes_validos = [m.upper() for m in NOMES_MUNICIPIOS]

    # TODO Exercicio 2: regra_valida = F.coalesce(..., F.lit(False))
    regra_valida = None

    rejeitadas = normalizado.filter(~regra_valida).withColumn(
        "motivo",
        F.lit(None).cast("string"),  # TODO Exercicio 2: cadeia de F.when(...).otherwise(...)
    )
    aprovadas = normalizado.filter(regra_valida)
    return aprovadas, rejeitadas


def enriquecer(aprovadas: DataFrame, municipios: DataFrame) -> DataFrame:
    """Exercicio 3 -- amplitude termica, faixa do dia, e juntar com a regiao.

    Acrescente duas colunas e junte com a tabela de municipios:
    - `amplitude_termica`: F.round(temperatura_max - temperatura_min, 1).
    - `faixa_dia`: "fria" quando temperatura_media < 15, "amena" quando
      <= 22, "quente" acima disso -- uma cadeia de F.when.
    - Depois, junte com `municipios` por `municipio` (on como string) para
      trazer a coluna `regiao`.

    Selecione ao final: estacao_id, data, municipio, regiao,
    temperatura_min, temperatura_max, temperatura_media, amplitude_termica,
    umidade_pct, faixa_dia.

    Resposta esperada: 886 linhas (mesma contagem das aprovadas), cada uma
    agora com `regiao`, `amplitude_termica` e `faixa_dia` preenchidos.
    """
    # TODO Exercicio 3: withColumn("amplitude_termica", ...), withColumn("faixa_dia", ...),
    # join com municipios, select final.
    return None


def resumir_por_regiao(limpo: DataFrame) -> DataFrame:
    """Exercicio 4 -- media trimestral de temperatura e amplitude por regiao.

    Agrupe `limpo` por `regiao` e calcule:
    - `leituras`: F.count("*").
    - `media_trimestral`: F.round(F.avg("temperatura_media"), 2).
    - `amplitude_media`: F.round(F.avg("amplitude_termica"), 2).

    Ordene decrescente por `media_trimestral`.

    Resposta esperada (°C, 2 casas):

        Grande Florianopolis     19.72
        Vale do Itajai           19.49
        Norte Catarinense        19.01
        Sul Catarinense          18.21
        Oeste Catarinense        17.66
        Serrana                  14.66
    """
    # TODO Exercicio 4: groupBy("regiao") + agg(...) + orderBy(F.desc(...))
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar local, depois subir para o Blob
# ---------------------------------------------------------------------------
def carregar(limpo: DataFrame, resumo_regiao: DataFrame, rejeitadas: DataFrame, container) -> None:
    destino_fato = SAIDA / "temperaturas"
    destino_resumo = SAIDA / "resumo_regiao"
    destino_rejeitadas = SAIDA / "temp_rejeitadas"

    (
        limpo
        .repartition("municipio")
        .write.mode("overwrite")
        .partitionBy("municipio")
        .parquet(str(destino_fato))
    )
    resumo_regiao.coalesce(1).write.mode("overwrite").parquet(str(destino_resumo))
    rejeitadas.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(destino_rejeitadas)
    )

    for pasta in (destino_fato, destino_resumo, destino_rejeitadas):
        arquivos = [
            p for p in pasta.rglob("*")
            if p.is_file() and not p.name.startswith(("_", "."))
        ]
        print(f"{pasta.name:<16} {len(arquivos):>3} arquivo(s) de dados (local)")

    # So agora o resultado sobe para o Blob -- o Spark nunca escreveu la
    # diretamente, porque `local[4]` nao tem um conector de sistema de
    # arquivos para o Blob configurado (veja o README, secao "por que baixar
    # em vez de ler direto").
    total_enviado = 0
    for pasta, prefixo in (
        (destino_fato, "processado/temperaturas"),
        (destino_resumo, "processado/resumo_regiao"),
        (destino_rejeitadas, "processado/temp_rejeitadas"),
    ):
        total_enviado += subir_pasta(container, pasta, prefixo)
    print(f"arquivos enviados ao Blob (processado/*): {total_enviado}")


def verificar(container) -> None:
    """Prova de ponta a ponta: o que esta no Blob depois do Load e legivel."""
    blobs_resumo = [
        b.name for b in container.list_blobs(name_starts_with="processado/resumo_regiao/")
        if b.name.endswith(".parquet")
    ]
    print(f"arquivo(s) de resumo no Blob: {blobs_resumo}")
    if not blobs_resumo:
        raise SystemExit("nenhum parquet de resumo encontrado no Blob -- o Load falhou")

    destino = DADOS / "_verificacao_resumo.parquet"
    baixar_arquivo(container, blobs_resumo[0], destino)
    print(f"baixado de volta do Blob para conferencia: {destino} ({destino.stat().st_size} bytes)")


def main() -> None:
    spark = criar_sessao("aula04-01-etl-temperaturas-blob")
    try:
        titulo("0. Fonte -- gerar e semear o Blob")
        gerar_csv_temperaturas()
        gerar_csv_municipios()
        container = obter_container_client()
        semear_blob(container)

        titulo("1. Extract -- baixar do Blob e ler com o Spark")
        bruto, municipios = extrair(spark, container)

        titulo("2. Transform -- normalizar (exercicio 1)")
        normalizado = normalizar(bruto)
        total_bruto = bruto.count()
        total_normalizado = normalizado.count()
        print(f"lidas .............: {total_bruto}")
        print(f"apos dropDuplicates: {total_normalizado}"
              f"  (-{total_bruto - total_normalizado} duplicatas)")

        titulo("3. Transform -- validar (exercicio 2)")
        aprovadas, rejeitadas = validar(normalizado)
        total_aprovado = aprovadas.count()
        total_rejeitado = rejeitadas.count()
        print(f"aprovadas .........: {total_aprovado}")
        print(f"rejeitadas ........: {total_rejeitado}")
        # Conciliacao: toda linha normalizada tem de sair de um dos dois lados.
        assert total_aprovado + total_rejeitado == total_normalizado, (
            f"linhas perdidas: {total_normalizado - total_aprovado - total_rejeitado}"
        )
        print("conciliacao .......: OK")
        rejeitadas.groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)

        titulo("4. Transform -- enriquecer (exercicio 3)")
        limpo = enriquecer(aprovadas, municipios)
        limpo.cache()
        limpo.show(5)

        titulo("5. Transform -- resumo por regiao (exercicio 4)")
        resumo_regiao = resumir_por_regiao(limpo)
        resumo_regiao.show()

        titulo("6. Load -- gravar local e subir para o Blob")
        carregar(limpo, resumo_regiao, rejeitadas, container)

        titulo("7. Verificacao -- ler de volta do Blob")
        verificar(container)

        titulo("Resumo da execucao")
        print(f"container ....: {container.container_name}")
        print(f"lidas ........: {total_bruto}")
        print(f"aprovadas ....: {total_aprovado}")
        print(f"rejeitadas ...: {total_rejeitado}")
        print(f"saida local ..: {SAIDA}")
        print("saida no Blob : processado/temperaturas, processado/resumo_regiao, "
              "processado/temp_rejeitadas")
    finally:
        spark.stop()


def limpar_saida() -> None:
    """Apaga a saida local, preservando a entrada -- util para reexecutar do zero."""
    if SAIDA.exists():
        shutil.rmtree(SAIDA)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_saida()
        print(f"saida local removida: {SAIDA}")
    main()
