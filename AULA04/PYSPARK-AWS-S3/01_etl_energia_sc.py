"""Exercicio de ETL -- consumo de energia de um trimestre em dez cidades de
SC, lido e gravado no S3.

    python 01_etl_energia_sc.py

Este arquivo E o exercicio: ele te da a Fonte (gerador do CSV e do envio ao
S3), o Extract, o Load e a Verificacao prontos, mas deixa quatro blocos de
Transform com `# TODO Exercicio N` no lugar do codigo. Leia o enunciado de
cada um no EXERCICIOS_AWS.md, escreva a linha que falta e rode de novo.

Enquanto um TODO nao for preenchido, a funcao devolve `None` e o script para
com um erro claro (`AttributeError: 'NoneType' object has no attribute ...`)
apontando exatamente qual bloco falta -- o mesmo mecanismo do exercicio de
ETL com Blob Storage da pasta ao lado (PYSPARK-AZURE-BLOB).

O padrao geral -- baixar do S3, processar local, subir o resultado de volta
-- ja esta pronto nas etapas de Extract e Load. O que falta escrever e a
parte que nao muda de nuvem para nuvem: normalizar, validar, enriquecer e
resumir um DataFrame comum.

    dados/energia_sc.csv              entrada local (gerada e enviada uma vez)
    dados/municipios_sc.csv           tabela de apoio (municipio -> regiao)
    S3 bruto/energia_sc.csv           a mesma entrada, no bucket
    S3 referencia/municipios_sc.csv   a mesma tabela, no bucket
    saida/energia/municipio=.../      fato limpo, local
    S3 processado/energia/...         o mesmo fato, subido de volta
    S3 processado/resumo_regiao/...   consumo e custo trimestral por regiao
    S3 processado/energia_rejeitada/  leituras que nao passaram na validacao
"""

import csv
import datetime
import random
import shutil

from comum import (
    BUCKET,
    DADOS,
    SAIDA,
    baixar_arquivo,
    criar_sessao,
    obter_bucket,
    objeto_existe,
    subir_arquivo,
    subir_pasta,
    titulo,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ARQUIVO_ENERGIA = DADOS / "energia_sc.csv"
ARQUIVO_MUNICIPIOS = DADOS / "municipios_sc.csv"

# municipio -> (regiao, populacao estimada)
MUNICIPIOS_REGIOES = {
    "Florianopolis": ("Grande Florianopolis", 522000),
    "Sao Jose": ("Grande Florianopolis", 254000),
    "Joinville": ("Norte Catarinense", 604000),
    "Jaragua do Sul": ("Norte Catarinense", 190000),
    "Blumenau": ("Vale do Itajai", 361000),
    "Itajai": ("Vale do Itajai", 224000),
    "Balneario Camboriu": ("Vale do Itajai", 145000),
    "Chapeco": ("Oeste Catarinense", 226000),
    "Criciuma": ("Sul Catarinense", 217000),
    "Lages": ("Serrana", 158000),
}
NOMES_MUNICIPIOS = list(MUNICIPIOS_REGIOES)

# setor -> (kWh base por 10 mil habitantes/dia, tarifa media R$/kWh)
SETORES = {
    "residencial": (1200.0, 0.75),
    "comercial": (700.0, 0.85),
    "industrial": (900.0, 0.55),
    "rural": (150.0, 0.60),
}
NOMES_SETORES = list(SETORES)

DATA_INICIO = datetime.date(2024, 7, 1)
DATA_FIM = datetime.date(2024, 9, 30)          # trimestre: 92 dias, jul-set

SCHEMA_ENERGIA = StructType(
    [
        StructField("leitura_id", IntegerType(), True),
        StructField("data", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("setor", StringType(), True),
        StructField("consumo_kwh", DoubleType(), True),
        StructField("unidades_consumidoras", DoubleType(), True),
        StructField("tarifa_rs_kwh", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- dado, sem exercicio: gera os dois CSVs localmente e semeia o S3
# ---------------------------------------------------------------------------
def gerar_csv_energia() -> None:
    if ARQUIVO_ENERGIA.exists():
        print(f"entrada ja existe: {ARQUIVO_ENERGIA} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(19)               # semente fixa: numeros reproduziveis
    dias = (DATA_FIM - DATA_INICIO).days + 1     # 92

    # unidades consumidoras: quase constante por (municipio, setor) -- varia
    # pouco dia a dia, como o numero real de clientes ligados a rede.
    uc_base = {}
    for municipio, (_, pop) in MUNICIPIOS_REGIOES.items():
        for setor in NOMES_SETORES:
            if setor == "residencial":
                uc_base[(municipio, setor)] = pop / 2.6
            elif setor == "comercial":
                uc_base[(municipio, setor)] = pop / 18
            elif setor == "industrial":
                uc_base[(municipio, setor)] = pop / 120
            else:
                uc_base[(municipio, setor)] = pop / 60

    cabecalho = [
        "leitura_id", "data", "municipio", "setor",
        "consumo_kwh", "unidades_consumidoras", "tarifa_rs_kwh",
    ]
    linhas = []
    leitura_id = 0
    for dia_idx in range(dias):
        data = DATA_INICIO + datetime.timedelta(days=dia_idx)
        tendencia = 1.0 + 0.15 * (dia_idx / (dias - 1))  # leve alta rumo a primavera
        for municipio, (_, pop) in MUNICIPIOS_REGIOES.items():
            for setor in NOMES_SETORES:
                leitura_id += 1
                base_kwh, tarifa_media = SETORES[setor]
                consumo = base_kwh * (pop / 10000) * tendencia * aleatorio.gauss(1.0, 0.08)
                consumo = round(max(0.0, consumo), 1)
                tarifa = round(max(0.1, aleatorio.gauss(tarifa_media, 0.03)), 3)
                uc = round(uc_base[(municipio, setor)] * aleatorio.gauss(1.0, 0.02))

                linha = [leitura_id, data.isoformat(), municipio, setor, consumo, uc, tarifa]

                # ~5% de sujeira, dos tipos que uma leitura de medidor produz de verdade
                sorteio = aleatorio.random()
                if sorteio < 0.01:
                    linha[2] = ""                            # municipio ausente
                elif sorteio < 0.02:
                    linha[2] = linha[2].lower()               # municipio em formato errado
                elif sorteio < 0.03:
                    linha[3] = "outro"                        # setor invalido
                elif sorteio < 0.04:
                    linha[4] = -abs(linha[4])                 # consumo negativo (erro de medicao)
                elif sorteio < 0.05:
                    linha[6] = 15.0 if aleatorio.random() < 0.5 else -0.5  # tarifa fora da faixa

                linhas.append(linha)
                if sorteio < 0.004:
                    linhas.append(list(linha))                # duplicata exata

    with ARQUIVO_ENERGIA.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    tamanho = ARQUIVO_ENERGIA.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_ENERGIA} ({len(linhas)} linhas, {tamanho:.0f} KB)")


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


def semear_s3(s3) -> None:
    """Sobe as duas entradas para o S3, se ainda nao estiverem la.

    Em produção alguem (ou algum outro pipeline) ja teria colocado o dado no
    bucket antes do seu job rodar. Aqui essa etapa e simulada uma unica vez
    -- depois disso, o restante do script trata o S3 como a fonte de
    verdade, sem olhar para o arquivo local de novo.
    """
    for local, remoto in (
        (ARQUIVO_ENERGIA, "bruto/energia_sc.csv"),
        (ARQUIVO_MUNICIPIOS, "referencia/municipios_sc.csv"),
    ):
        if objeto_existe(s3, remoto):
            print(f"ja esta no S3: {remoto} (nao reenviado)")
            continue
        subir_arquivo(s3, local, remoto)
        print(f"enviado ao S3: {local.name} -> {remoto}")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: baixar do S3, so entao ler com o Spark
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession, s3) -> tuple[DataFrame, DataFrame]:
    # O Spark local nao fala com o S3 diretamente -- ele le do disco. Por
    # isso o download vem antes do `spark.read`, nao dentro dele.
    baixar_arquivo(s3, "bruto/energia_sc.csv", DADOS / "_baixado_energia.csv")
    baixar_arquivo(s3, "referencia/municipios_sc.csv", DADOS / "_baixado_municipios.csv")

    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_ENERGIA)
        .csv(str(DADOS / "_baixado_energia.csv"))
    )
    municipios = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(DADOS / "_baixado_municipios.csv"))
    )
    print("linhas baixadas do S3 (energia) ....:", bruto.count())
    print("linhas baixadas do S3 (municipios) .:", municipios.count())
    bruto.show(5, truncate=False)
    return bruto, municipios


# ---------------------------------------------------------------------------
# T de Transform -- quatro blocos, quatro exercicios
# ---------------------------------------------------------------------------
def normalizar(bruto: DataFrame) -> DataFrame:
    """Exercicio 1 -- padronizar municipio e setor, converter a data e tirar duplicatas.

    Quatro coisas, nesta ordem:
    1. `municipio` para maiusculo e sem espaco nas pontas.
    2. `setor` para minusculo e sem espaco nas pontas (aqui o padrao e
       minusculo, nao maiusculo -- e o valor de comparacao com NOMES_SETORES).
    3. `data` de string para date, com F.to_date("data", "yyyy-MM-dd").
    4. dropDuplicates em ["leitura_id", "data", "municipio", "setor"].

    Resposta esperada: 3680 linhas (13 duplicatas removidas de 3693 lidas).
    """
    # TODO Exercicio 1: normalizar municipio e setor, converter a data, remover duplicatas.
    return None


def validar(normalizado: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Exercicio 2 -- separar leituras validas de invalidas, com o motivo.

    Uma leitura e valida quando, ao mesmo tempo:
    - `municipio` esta em NOMES_MUNICIPIOS (compare em maiusculo);
    - `setor` esta em NOMES_SETORES;
    - `consumo_kwh` >= 0;
    - `tarifa_rs_kwh` esta dentro de [0.2, 2.0];
    - `unidades_consumidoras` > 0.

    Junte as cinco condicoes com `&` dentro de um F.coalesce(..., F.lit(False))
    -- exatamente como a `regra_valida` do exercicio de temperaturas da
    pasta ao lado. Sem o coalesce, uma condicao que vira NULL faz a linha
    inteira sumir dos dois lados sem que a conciliacao note.

    Monte a coluna `motivo` nos rejeitados com uma cadeia de F.when, nesta
    ordem de prioridade: municipio invalido, setor invalido, consumo
    negativo, tarifa fora da faixa, e o que sobrar (`.otherwise(...)`) e
    unidades consumidoras invalidas.

    Resposta esperada: 3538 aprovadas, 142 rejeitadas.

        setor invalido                                44
        municipio invalida ou ausente                 35
        tarifa fora da faixa (0.2 a 2.0)               35
        consumo negativo                               28
        unidades consumidoras invalidas                 0
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
    """Exercicio 3 -- custo total, faixa de consumo, e juntar com a regiao.

    Acrescente duas colunas e junte com a tabela de municipios:
    - `custo_total`: F.round(consumo_kwh * tarifa_rs_kwh, 2).
    - `faixa_consumo`: "baixo" quando consumo_kwh < 5000, "medio" quando
      <= 20000, "alto" acima disso -- uma cadeia de F.when.
    - Depois, junte com `municipios` por `municipio` (on como string) para
      trazer a coluna `regiao`.

    Selecione ao final: leitura_id, data, municipio, regiao, setor,
    consumo_kwh, unidades_consumidoras, tarifa_rs_kwh, custo_total,
    faixa_consumo.

    Resposta esperada: 3538 linhas (mesma contagem das aprovadas). Faixas:
    alto 1653, medio 1260, baixo 625.
    """
    # TODO Exercicio 3: withColumn("custo_total", ...), withColumn("faixa_consumo", ...),
    # join com municipios, select final.
    return None


def resumir_por_regiao(limpo: DataFrame) -> DataFrame:
    """Exercicio 4 -- consumo medio e custo total trimestral por regiao.

    Agrupe `limpo` por `regiao` e calcule:
    - `leituras`: F.count("*").
    - `consumo_medio_kwh`: F.round(F.avg("consumo_kwh"), 2).
    - `custo_total_rs`: F.round(F.sum("custo_total"), 2).

    Ordene decrescente por `custo_total_rs`.

    Resposta esperada (R$, trimestre inteiro):

        Norte Catarinense        15698326.31
        Grande Florianopolis     15259130.16
        Vale do Itajai           14393176.59
        Oeste Catarinense         4362617.22
        Sul Catarinense           4354908.94
        Serrana                   3174153.25
    """
    # TODO Exercicio 4: groupBy("regiao") + agg(...) + orderBy(F.desc(...))
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar local, depois subir para o S3
# ---------------------------------------------------------------------------
def carregar(limpo: DataFrame, resumo_regiao: DataFrame, rejeitadas: DataFrame, s3) -> None:
    destino_fato = SAIDA / "energia"
    destino_resumo = SAIDA / "resumo_regiao"
    destino_rejeitadas = SAIDA / "energia_rejeitada"

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
        print(f"{pasta.name:<18} {len(arquivos):>3} arquivo(s) de dados (local)")

    # So agora o resultado sobe para o S3 -- o Spark nunca escreveu la
    # diretamente, porque `local[4]` nao tem um conector de sistema de
    # arquivos para o S3 configurado (veja o README, secao "por que baixar
    # em vez de ler direto").
    total_enviado = 0
    for pasta, prefixo in (
        (destino_fato, "processado/energia"),
        (destino_resumo, "processado/resumo_regiao"),
        (destino_rejeitadas, "processado/energia_rejeitada"),
    ):
        total_enviado += subir_pasta(s3, pasta, prefixo)
    print(f"arquivos enviados ao S3 (processado/*): {total_enviado}")


def verificar(s3) -> None:
    """Prova de ponta a ponta: o que esta no S3 depois do Load e legivel."""
    resposta = s3.list_objects_v2(Bucket=BUCKET, Prefix="processado/resumo_regiao/")
    chaves_resumo = [
        obj["Key"] for obj in resposta.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    print(f"objeto(s) de resumo no S3: {chaves_resumo}")
    if not chaves_resumo:
        raise SystemExit("nenhum parquet de resumo encontrado no S3 -- o Load falhou")

    destino = DADOS / "_verificacao_resumo.parquet"
    baixar_arquivo(s3, chaves_resumo[0], destino)
    print(f"baixado de volta do S3 para conferencia: {destino} ({destino.stat().st_size} bytes)")


def main() -> None:
    spark = criar_sessao("aula04-01-etl-energia-s3")
    try:
        titulo("0. Fonte -- gerar e semear o S3")
        gerar_csv_energia()
        gerar_csv_municipios()
        s3 = obter_bucket()
        semear_s3(s3)

        titulo("1. Extract -- baixar do S3 e ler com o Spark")
        bruto, municipios = extrair(spark, s3)

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

        titulo("6. Load -- gravar local e subir para o S3")
        carregar(limpo, resumo_regiao, rejeitadas, s3)

        titulo("7. Verificacao -- ler de volta do S3")
        verificar(s3)

        titulo("Resumo da execucao")
        print(f"bucket .......: {BUCKET}")
        print(f"lidas ........: {total_bruto}")
        print(f"aprovadas ....: {total_aprovado}")
        print(f"rejeitadas ...: {total_rejeitado}")
        print(f"saida local ..: {SAIDA}")
        print("saida no S3   : processado/energia, processado/resumo_regiao, "
              "processado/energia_rejeitada")
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
