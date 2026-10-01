"""Gera dados rotulados de churn e demanda para a trilha supervisionada."""

import csv
import math
import random

from comum import BRONZE, DADOS, criar_sessao, titulo


def gerar_fontes() -> None:
    DADOS.mkdir(parents=True, exist_ok=True)

    assinaturas = DADOS / "assinaturas.csv"
    if not assinaturas.exists():
        rng = random.Random(171)
        with assinaturas.open("w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([
                "cliente_id", "tenure_meses", "mensalidade",
                "chamados_suporte", "contrato_meses", "cancelou",
            ])
            for cliente_id in range(1, 1001):
                tenure = rng.randint(1, 72)
                mensalidade = round(rng.uniform(35, 180), 2)
                chamados = rng.randint(0, 8)
                contrato = rng.choice([1, 1, 1, 12, 24])
                risco = (
                    -2.2 - 0.045 * tenure + 0.018 * mensalidade
                    + 0.48 * chamados - 0.055 * contrato
                )
                chance = 1 / (1 + math.exp(-risco))
                writer.writerow([
                    cliente_id, tenure, mensalidade, chamados, contrato,
                    int(rng.random() < chance),
                ])

    energia = DADOS / "demanda_energia.csv"
    if not energia.exists():
        rng = random.Random(172)
        with energia.open("w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([
                "registro_id", "temperatura_c", "dia_semana", "feriado",
                "ocupacao_pct", "demanda_mwh",
            ])
            for registro_id in range(1, 1201):
                temperatura = round(rng.uniform(4, 38), 1)
                dia = rng.randint(0, 6)
                feriado = int(dia < 5 and rng.random() < 0.08)
                ocupacao = round(rng.uniform(25, 100), 1)
                demanda = 42 + 1.7 * max(0, 18 - temperatura)
                demanda += 1.25 * max(0, temperatura - 22)
                demanda += 0.32 * ocupacao + (8 if dia < 5 else -9)
                demanda -= 12 * feriado
                demanda += rng.gauss(0, 5)
                writer.writerow([
                    registro_id, temperatura, dia, feriado, ocupacao,
                    round(demanda, 2),
                ])


def main() -> None:
    gerar_fontes()
    spark = criar_sessao("aula06-supervisionado-01-bronze")
    try:
        for nome in ("assinaturas", "demanda_energia"):
            arquivo = DADOS / f"{nome}.csv"
            bruto = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(arquivo))
            )
            destino = BRONZE / nome
            bruto.write.mode("overwrite").parquet(str(destino))
            print(f"{nome}: {bruto.count()} linhas -> {destino}")
        titulo("Bronze pronta: fontes com rotulos preservadas")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
