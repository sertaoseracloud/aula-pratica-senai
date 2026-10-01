"""Gera perfis sinteticos de clientes sem rótulo-alvo e carrega Bronze."""

import csv
import random

from comum import BRONZE, DADOS, criar_sessao, titulo

ARQUIVO = DADOS / "comportamento_clientes.csv"


def gerar_csv() -> None:
    if ARQUIVO.exists():
        print(f"entrada ja existe: {ARQUIVO} (nao regenerada)")
        return
    DADOS.mkdir(parents=True, exist_ok=True)
    rng = random.Random(281)
    perfis = [
        (90, 3, 35, 4, 0.04),
        (260, 10, 16, 12, 0.08),
        (620, 18, 6, 25, 0.12),
    ]
    with ARQUIVO.open("w", newline="", encoding="utf-8") as arquivo:
        writer = csv.writer(arquivo)
        writer.writerow([
            "cliente_id", "gasto_mensal", "compras_mes",
            "dias_desde_ultima_compra", "visitas_mes", "devolucoes_pct",
        ])
        cliente_id = 1
        for (
            gasto_mu, compras_mu, recencia_mu, visitas_mu, devolucoes_mu
        ) in perfis:
            for _ in range(300):
                gasto = max(10, rng.gauss(gasto_mu, gasto_mu * 0.15))
                variacao_compras = compras_mu * 0.16
                compras = max(
                    1, round(rng.gauss(compras_mu, variacao_compras))
                )
                recencia = max(1, round(rng.gauss(recencia_mu, 4)))
                variacao_visitas = visitas_mu * 0.18
                visitas = max(
                    1, round(rng.gauss(visitas_mu, variacao_visitas))
                )
                devolucoes = min(
                    0.5, max(0, rng.gauss(devolucoes_mu, 0.025))
                )
                writer.writerow([
                    cliente_id, round(gasto, 2), compras, recencia, visitas,
                    round(devolucoes, 3),
                ])
                cliente_id += 1


def main() -> None:
    gerar_csv()
    spark = criar_sessao("aula06-nao-supervisionado-01-bronze")
    try:
        bruto = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(ARQUIVO))
        )
        destino = BRONZE / "comportamento_clientes"
        bruto.write.mode("overwrite").parquet(str(destino))
        print(f"Bronze: {bruto.count()} clientes -> {destino}")
        titulo("Bronze pronta: nenhum rotulo-alvo foi gerado")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
