"""Limpa e prepara as tres fontes para treinamento e inferencia."""

from __future__ import annotations

from comum import BRONZE, SILVER, criar_sessao, titulo
from pyspark.sql import functions as F


def main() -> None:
    spark = criar_sessao("aula06-02-silver")
    try:
        tarefas = {
            "assinaturas": (
                [
                    "tenure_meses", "mensalidade", "chamados_suporte",
                    "contrato_meses",
                ],
                (F.col("tenure_meses").between(1, 120))
                & (F.col("mensalidade") > 0)
                & (F.col("chamados_suporte") >= 0)
                & F.col("cancelou").isin(0, 1),
            ),
            "demanda_energia": (
                [
                    "temperatura_c", "dia_semana", "feriado",
                    "ocupacao_pct",
                ],
                F.col("dia_semana").between(0, 6)
                & F.col("feriado").isin(0, 1)
                & F.col("ocupacao_pct").between(0, 100)
                & (F.col("demanda_mwh") > 0),
            ),
            "clientes_varejo": (
                ["gasto_mensal", "compras_mes", "dias_desde_ultima_compra"],
                (F.col("gasto_mensal") > 0)
                & (F.col("compras_mes") > 0)
                & (F.col("dias_desde_ultima_compra") >= 0),
            ),
        }
        for nome, (colunas_modelo, regra_valida) in tarefas.items():
            origem = BRONZE / nome
            if not origem.exists():
                raise SystemExit(
                    f"Bronze ausente: {origem}. Rode 01_bronze_ingestao.py."
                )
            bruto = spark.read.parquet(str(origem))
            limpo = bruto.dropDuplicates().filter(
                F.coalesce(regra_valida, F.lit(False))
            )
            if nome == "clientes_varejo":
                gasto_por_compra = F.round(
                    F.col("gasto_mensal") / F.col("compras_mes"), 2
                )
                limpo = limpo.withColumn("gasto_por_compra", gasto_por_compra)
                colunas_modelo = [*colunas_modelo, "gasto_por_compra"]
            destino = SILVER / nome
            limpo.write.mode("overwrite").parquet(str(destino))
            rejeitadas = bruto.count() - limpo.count()
            print(
                f"{nome}: {limpo.count()} aprovadas, {rejeitadas} removidas; "
                f"features: {', '.join(colunas_modelo)}"
            )
        titulo("Silver pronta: dados validados e features definidas")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
