"""Valida as duas fontes rotuladas e audita os registros rejeitados."""

from comum import BRONZE, SILVER, criar_sessao, titulo
from pyspark.sql import functions as F


def persistir_resultado(bruto, nome: str, regra_valida):
    normalizado = bruto.dropDuplicates()
    valida = F.coalesce(regra_valida, F.lit(False))
    aprovadas = normalizado.filter(valida)
    rejeitadas = normalizado.filter(~valida).withColumn(
        "motivo",
        F.lit("campo ausente ou fora da faixa valida"),
    )
    total = normalizado.count()
    total_ok = aprovadas.count()
    total_rejeitado = rejeitadas.count()
    assert total_ok + total_rejeitado == total, "linhas nao conciliadas"
    aprovadas.write.mode("overwrite").parquet(str(SILVER / nome))
    rejeitadas.write.mode("overwrite").parquet(
        str(SILVER / f"{nome}_rejeitadas")
    )
    print(f"{nome}: {total_ok} aprovadas, {total_rejeitado} rejeitadas")


def main() -> None:
    spark = criar_sessao("aula06-supervisionado-02-silver")
    regras = {
        "assinaturas": (
            (F.col("tenure_meses").between(1, 120))
            & (F.col("mensalidade") > 0)
            & (F.col("chamados_suporte") >= 0)
            & F.col("cancelou").isin(0, 1)
        ),
        "demanda_energia": (
            F.col("dia_semana").between(0, 6)
            & F.col("feriado").isin(0, 1)
            & F.col("ocupacao_pct").between(0, 100)
            & (F.col("demanda_mwh") > 0)
        ),
    }
    try:
        for nome, regra in regras.items():
            origem = BRONZE / nome
            if not origem.exists():
                raise SystemExit(
                    f"Bronze ausente em {origem}. Rode 01_bronze_ingestao.py."
                )
            persistir_resultado(spark.read.parquet(str(origem)), nome, regra)
        titulo("Silver pronta: dados rotulados validados")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
