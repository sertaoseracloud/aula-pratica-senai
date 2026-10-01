"""Valida as leituras e separa os registros rejeitados com motivo."""

from comum import BRONZE, SILVER, criar_sessao, titulo
from pyspark.sql import functions as F


def main() -> None:
    spark = criar_sessao("aula06-parte3-02-silver")
    try:
        origem = BRONZE / "sensores_maquinas"
        if not origem.exists():
            raise SystemExit(
                f"Bronze ausente em {origem}. Rode 01_bronze_ingestao.py."
            )
        bruto = spark.read.parquet(str(origem))
        normalizado = bruto.dropDuplicates(["leitura_id"])

        regra_valida = F.coalesce(
            F.col("temperatura_c").between(-20, 150)
            & F.col("vibracao_mm_s").between(0, 12)
            & F.col("pressao_bar").between(0, 200)
            & F.col("horas_desde_manutencao").between(0, 50000)
            & F.col("carga_pct").between(0, 100)
            & F.col("falha_24h").isin(0, 1),
            F.lit(False),
        )
        aprovadas = normalizado.filter(regra_valida)
        rejeitadas = normalizado.filter(~regra_valida).withColumn(
            "motivo",
            F.when(
                ~F.col("temperatura_c").between(-20, 150),
                "temperatura fora da faixa",
            )
            .when(
                ~F.col("vibracao_mm_s").between(0, 12),
                "vibracao fora da faixa",
            )
            .when(
                ~F.col("pressao_bar").between(0, 200),
                "pressao fora da faixa",
            )
            .when(
                ~F.col("horas_desde_manutencao").between(0, 50000),
                "horas desde manutencao fora da faixa",
            )
            .when(
                ~F.col("carga_pct").between(0, 100),
                "carga fora da faixa",
            )
            .otherwise("rotulo falha_24h invalido"),
        )

        total = normalizado.count()
        total_ok = aprovadas.count()
        total_rejeitado = rejeitadas.count()
        assert total_ok + total_rejeitado == total, "linhas nao conciliadas"
        aprovadas.write.mode("overwrite").parquet(
            str(SILVER / "sensores_maquinas")
        )
        rejeitadas.write.mode("overwrite").parquet(
            str(SILVER / "sensores_maquinas_rejeitadas")
        )
        print(
            f"Silver: {total_ok} aprovadas, {total_rejeitado} rejeitadas; "
            "conciliacao OK"
        )
        rejeitadas.groupBy("motivo").count().show(truncate=False)
        titulo("Silver pronta: dados validos e rejeicoes auditaveis")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
