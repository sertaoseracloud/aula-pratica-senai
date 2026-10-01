"""Valida perfis sem alvo e cria uma feature derivada para agrupamento."""

from comum import BRONZE, SILVER, criar_sessao, titulo
from pyspark.sql import functions as F


def main() -> None:
    spark = criar_sessao("aula06-nao-supervisionado-02-silver")
    try:
        origem = BRONZE / "comportamento_clientes"
        if not origem.exists():
            raise SystemExit(
                f"Bronze ausente em {origem}. Rode 01_bronze_ingestao.py."
            )
        bruto = spark.read.parquet(str(origem))
        normalizado = bruto.dropDuplicates(["cliente_id"])
        regra_valida = F.coalesce(
            (F.col("gasto_mensal") > 0)
            & (F.col("compras_mes") > 0)
            & F.col("dias_desde_ultima_compra").between(0, 365)
            & (F.col("visitas_mes") > 0)
            & F.col("devolucoes_pct").between(0, 1),
            F.lit(False),
        )
        aprovadas = normalizado.filter(regra_valida).withColumn(
            "gasto_por_compra",
            F.round(F.col("gasto_mensal") / F.col("compras_mes"), 2),
        )
        rejeitadas = normalizado.filter(~regra_valida).withColumn(
            "motivo", F.lit("campo ausente ou fora da faixa valida")
        )
        total = normalizado.count()
        total_ok = aprovadas.count()
        total_rejeitado = rejeitadas.count()
        assert total_ok + total_rejeitado == total, "linhas nao conciliadas"
        aprovadas.write.mode("overwrite").parquet(
            str(SILVER / "comportamento_clientes")
        )
        rejeitadas.write.mode("overwrite").parquet(
            str(SILVER / "comportamento_clientes_rejeitadas")
        )
        print(
            f"Silver: {total_ok} aprovadas, {total_rejeitado} rejeitadas; "
            "conciliacao OK"
        )
        titulo("Silver pronta: atributos sem rótulo para aprendizado")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
