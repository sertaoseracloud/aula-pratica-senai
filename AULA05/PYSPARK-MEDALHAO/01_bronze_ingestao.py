"""Job 1/3 -- camada Bronze: ingestao crua da qualidade do ar, com proveniencia.

    python 01_bronze_ingestao.py

A Bronze e o "deposito" da arquitetura medalhao: guarda o dado exatamente
como chegou, sem limpar nada -- inclusive as duplicatas e as leituras
fisicamente impossiveis. A unica coisa que ela acrescenta e PROVENIENCIA: de
qual arquivo veio, e quando foi ingerido. Se um numero furado aparecer duas
camadas depois, a pergunta "isso veio de onde e quando" se responde aqui,
porque a camada seguinte ja filtrou o problema para sempre.

Este e um job INDEPENDENTE: le do disco (`dados/`) e escreve em
`camadas/bronze/`. Roda sozinho, sem depender de nenhum outro job. Quem
depende dele e o job seguinte (02_silver_limpeza.py), que le o que ESTE job
gravou -- nunca volta ao CSV original.

    dados/qualidade_ar_sc.csv     entrada (gerada na primeira execucao, 92 dias x 10 cidades)
    camadas/bronze/qualidade_ar/  saida: o mesmo dado, mais duas colunas de proveniencia
"""

import csv
import datetime
import random
import shutil

from comum import BRONZE, CAMADAS, DADOS, criar_sessao, titulo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ARQUIVO_AR = DADOS / "qualidade_ar_sc.csv"

# municipio -> pm2.5 base do trimestre, em ug/m3
MUNICIPIOS_PM25_BASE = {
    "Florianopolis": 12.0,
    "Sao Jose": 16.0,
    "Joinville": 22.0,
    "Jaragua do Sul": 17.0,
    "Blumenau": 20.0,
    "Itajai": 19.0,
    "Balneario Camboriu": 14.0,
    "Chapeco": 15.0,
    "Criciuma": 23.0,
    "Lages": 18.0,
}
NOMES_MUNICIPIOS = list(MUNICIPIOS_PM25_BASE)

DATA_INICIO = datetime.date(2024, 7, 1)
DATA_FIM = datetime.date(2024, 9, 30)          # trimestre: 92 dias, jul-set

SCHEMA_AR = StructType(
    [
        StructField("leitura_id", IntegerType(), True),
        StructField("data", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("co_ppm", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Fonte -- dado, sem exercicio: gera o CSV bruto uma unica vez
# ---------------------------------------------------------------------------
def gerar_csv_qualidade_ar() -> None:
    if ARQUIVO_AR.exists():
        print(f"entrada ja existe: {ARQUIVO_AR} (nao regerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    aleatorio = random.Random(31)               # semente fixa: numeros reproduziveis
    dias = (DATA_FIM - DATA_INICIO).days + 1     # 92

    cabecalho = ["leitura_id", "data", "municipio", "pm25", "pm10", "co_ppm"]
    linhas = []
    leitura_id = 0
    for dia_idx in range(dias):
        data = DATA_INICIO + datetime.timedelta(days=dia_idx)
        # inversao termica mais forte no inicio do inverno, melhora rumo a primavera
        tendencia = 1.15 - 0.30 * (dia_idx / (dias - 1))
        for municipio, base_pm25 in MUNICIPIOS_PM25_BASE.items():
            leitura_id += 1
            pm25 = max(1.0, base_pm25 * tendencia * aleatorio.gauss(1.0, 0.15))
            pm10 = pm25 * aleatorio.uniform(1.6, 2.0) + aleatorio.gauss(0, 2)
            pm10 = max(pm25, pm10)
            co = max(0.05, aleatorio.gauss(0.4 + pm25 / 80, 0.1))
            pm25 = round(pm25, 1)
            pm10 = round(pm10, 1)
            co = round(co, 2)

            linha = [leitura_id, data.isoformat(), municipio, pm25, pm10, co]

            # ~5% de sujeira, dos tipos que um sensor de qualidade do ar produz de verdade
            sorteio = aleatorio.random()
            if sorteio < 0.01:
                linha[2] = ""                            # municipio ausente
            elif sorteio < 0.02:
                linha[2] = linha[2].lower()               # municipio em formato errado
            elif sorteio < 0.03:
                linha[3], linha[4] = linha[4], linha[3]   # pm25/pm10 trocados (sensor invertido)
            elif sorteio < 0.04:
                linha[4] = 650.0 if aleatorio.random() < 0.5 else -10.0  # pm10 fora da faixa fisica
            elif sorteio < 0.05:
                linha[5] = 80.0 if aleatorio.random() < 0.5 else -1.0   # co fora da faixa

            linhas.append(linha)
            if sorteio < 0.004:
                linhas.append(list(linha))                # duplicata exata

    with ARQUIVO_AR.open("w", newline="", encoding="utf-8") as fh:
        escritor = csv.writer(fh)
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    tamanho = ARQUIVO_AR.stat().st_size / 1024
    print(f"entrada gerada: {ARQUIVO_AR} ({len(linhas)} linhas, {tamanho:.0f} KB)")


# ---------------------------------------------------------------------------
# E de Extract -- dado, sem exercicio: ler o CSV bruto com schema fixo
# ---------------------------------------------------------------------------
def extrair(spark: SparkSession) -> DataFrame:
    bruto = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SCHEMA_AR)
        .csv(str(ARQUIVO_AR))
    )
    print("linhas lidas ....:", bruto.count())
    bruto.show(5, truncate=False)
    return bruto


# ---------------------------------------------------------------------------
# O exercicio desta camada: proveniencia, nao limpeza
# ---------------------------------------------------------------------------
def enriquecer_proveniencia(bruto: DataFrame) -> DataFrame:
    """Exercicio 1 -- acrescentar de onde veio e quando chegou.

    A Bronze NAO valida, NAO normaliza, NAO remove duplicata -- isso e
    trabalho da Silver. O unico acrescimo aqui e proveniencia:

    - `arquivo_origem`: F.lit(ARQUIVO_AR.name) -- o nome do arquivo de onde
      esta leitura veio.
    - `ingerido_em`: F.current_timestamp() -- o instante em que este job
      rodou.

    Resposta esperada: 923 linhas (identico ao CSV bruto -- a Bronze nao
    filtra nada, so acrescenta duas colunas).
    """
    # TODO Exercicio 1: withColumn("arquivo_origem", ...), withColumn("ingerido_em", ...)
    return None


# ---------------------------------------------------------------------------
# L de Load -- dado, sem exercicio: gravar a Bronze
# ---------------------------------------------------------------------------
def carregar_bronze(bronze: DataFrame) -> None:
    destino = BRONZE / "qualidade_ar"
    # Sem particionamento: a Bronze e pouca coisa por enquanto (92 dias) e
    # ninguem filtra por coluna nela -- quem consome a Bronze e sempre o
    # proximo job inteiro, nao uma consulta seletiva.
    bronze.write.mode("overwrite").parquet(str(destino))

    arquivos = [
        p for p in destino.rglob("*")
        if p.is_file() and not p.name.startswith(("_", "."))
    ]
    print(f"Bronze gravada em {destino} ({len(arquivos)} arquivo(s) de dados)")


def main() -> None:
    spark = criar_sessao("aula05-01-bronze-ingestao")
    try:
        titulo("0. Fonte -- gerar o CSV bruto")
        gerar_csv_qualidade_ar()

        titulo("1. Extract -- ler o CSV bruto")
        bruto = extrair(spark)

        titulo("2. Proveniencia (exercicio 1)")
        bronze = enriquecer_proveniencia(bruto)
        total_bronze = bronze.count()
        print(f"linhas na Bronze ..: {total_bronze}")
        bronze.show(5, truncate=False)

        titulo("3. Load -- gravar a Bronze")
        carregar_bronze(bronze)

        titulo("Resumo do job")
        print(f"entrada ......: {ARQUIVO_AR}")
        print(f"linhas ativado: {total_bronze}")
        print(f"saida ........: {BRONZE / 'qualidade_ar'}")
        print("proximo job ..: 02_silver_limpeza.py")
    finally:
        spark.stop()


def limpar_camadas() -> None:
    """Apaga as tres camadas, preservando a entrada -- util para reprocessar do zero."""
    if CAMADAS.exists():
        shutil.rmtree(CAMADAS)


if __name__ == "__main__":
    import sys

    if "--limpar" in sys.argv:
        limpar_camadas()
        print(f"camadas removidas: {CAMADAS}")
    main()
