"""Gera leituras sinteticas de sensores e carrega os dados na Bronze."""

import csv
import math
import random

from comum import BRONZE, DADOS, criar_sessao, titulo

ARQUIVO = DADOS / "sensores_maquinas.csv"


def gerar_csv() -> None:
    if ARQUIVO.exists():
        print(f"entrada ja existe: {ARQUIVO} (nao regenerada)")
        return

    DADOS.mkdir(parents=True, exist_ok=True)
    rng = random.Random(631)
    with ARQUIVO.open("w", newline="", encoding="utf-8") as arquivo:
        writer = csv.writer(arquivo)
        writer.writerow([
            "leitura_id", "maquina_id", "temperatura_c", "vibracao_mm_s",
            "pressao_bar", "horas_desde_manutencao", "carga_pct",
            "falha_24h",
        ])
        for leitura_id in range(1, 2401):
            temperatura = round(rng.uniform(45, 100), 1)
            vibracao = round(rng.uniform(0.5, 10), 2)
            pressao = round(rng.uniform(85, 120), 1)
            horas = rng.randint(0, 1000)
            carga = round(rng.uniform(20, 100), 1)
            risco = -6.0 + 0.08 * max(0, temperatura - 72)
            risco += 0.7 * max(0, vibracao - 5)
            risco += 0.004 * max(0, horas - 400)
            risco += 0.6 if pressao < 92 else 0
            risco += 0.025 * max(0, carga - 75)
            probabilidade = 1 / (1 + math.exp(-risco))
            falha = int(rng.random() < probabilidade)

            if rng.random() < 0.01:
                vibracao = 15.0
            elif rng.random() < 0.01:
                pressao = -2.0

            writer.writerow([
                leitura_id, (leitura_id - 1) % 60 + 1, temperatura,
                vibracao, pressao, horas, carga, falha,
            ])


def main() -> None:
    gerar_csv()
    spark = criar_sessao("aula06-parte3-01-bronze")
    try:
        bruto = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(ARQUIVO))
        )
        destino = BRONZE / "sensores_maquinas"
        bruto.write.mode("overwrite").parquet(str(destino))
        print(f"Bronze: {bruto.count()} leituras -> {destino}")
        titulo("Bronze pronta: leituras originais preservadas")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
