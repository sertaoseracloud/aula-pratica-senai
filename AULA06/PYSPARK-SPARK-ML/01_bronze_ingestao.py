"""Gera fontes sinteticas e as preserva sem transformacao na Bronze."""

import csv
import math
import random

from comum import BRONZE, DADOS, criar_sessao, titulo


def gerar_fontes() -> None:
    DADOS.mkdir(parents=True, exist_ok=True)

    churn_path = DADOS / "assinaturas.csv"
    if not churn_path.exists():
        rng = random.Random(106)
        with churn_path.open("w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([
                "cliente_id", "tenure_meses", "mensalidade",
                "chamados_suporte", "contrato_meses", "cancelou",
            ])
            for cliente_id in range(1, 1201):
                tenure = rng.randint(1, 72)
                mensalidade = round(rng.uniform(35, 180), 2)
                chamados = rng.randint(0, 8)
                contrato = rng.choice([1, 1, 1, 12, 24])
                risco = (
                    -2.2 - 0.045 * tenure + 0.018 * mensalidade
                    + 0.48 * chamados - 0.055 * contrato
                )
                probabilidade = 1 / (1 + math.exp(-risco))
                cancelou = int(rng.random() < probabilidade)
                writer.writerow([
                    cliente_id, tenure, mensalidade, chamados, contrato,
                    cancelou,
                ])

    energia_path = DADOS / "demanda_energia.csv"
    if not energia_path.exists():
        rng = random.Random(206)
        with energia_path.open("w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([
                "registro_id", "temperatura_c", "dia_semana", "feriado",
                "ocupacao_pct", "demanda_mwh",
            ])
            for registro_id in range(1, 1501):
                temperatura = round(rng.uniform(4, 38), 1)
                dia_semana = rng.randint(0, 6)
                feriado = rng.randint(0, 1) if dia_semana < 5 else 0
                ocupacao = round(rng.uniform(25, 100), 1)
                demanda = (
                    42 + 1.7 * max(0, 18 - temperatura)
                    + 1.25 * max(0, temperatura - 22)
                )
                demanda += (
                    0.32 * ocupacao + (8 if dia_semana < 5 else -9)
                    - 12 * feriado
                )
                demanda += rng.gauss(0, 5)
                writer.writerow([
                    registro_id, temperatura, dia_semana, feriado,
                    ocupacao, round(demanda, 2),
                ])

    varejo_path = DADOS / "clientes_varejo.csv"
    if not varejo_path.exists():
        rng = random.Random(306)
        grupos = [
            (75, 3, 45),
            (260, 10, 18),
            (620, 18, 7),
        ]
        with varejo_path.open("w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([
                "cliente_id", "gasto_mensal", "compras_mes",
                "dias_desde_ultima_compra",
            ])
            cliente_id = 1
            for gasto_medio, compras_media, recencia_media in grupos:
                for _ in range(400):
                    gasto = max(10, rng.gauss(gasto_medio, gasto_medio * 0.16))
                    desvio_compras = max(1, compras_media * 0.18)
                    compras = max(
                        1, int(rng.gauss(compras_media, desvio_compras))
                    )
                    recencia = max(1, int(rng.gauss(recencia_media, 4)))
                    writer.writerow([
                        cliente_id, round(gasto, 2), compras, recencia
                    ])
                    cliente_id += 1


def main() -> None:
    gerar_fontes()
    spark = criar_sessao("aula06-01-bronze")
    try:
        for nome in ("assinaturas", "demanda_energia", "clientes_varejo"):
            origem = DADOS / f"{nome}.csv"
            destino = BRONZE / nome
            df = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(origem))
            )
            df.write.mode("overwrite").parquet(str(destino))
            print(f"{nome}: {df.count()} linhas -> {destino}")
        titulo("Bronze pronta: fontes originais preservadas em Parquet")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
