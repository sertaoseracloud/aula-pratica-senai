"""Valida os tres datasets e prepara os campos para Spark ML."""

from comum import BRONZE, SILVER, criar_sessao, titulo
from pyspark.sql import functions as F


def main() -> None:
    spark = criar_sessao("aula06-parte2-02-silver")
    regras = {
        "transacoes": (
            (F.col("valor") > 0)
            & F.col("hora").between(0, 23)
            & (F.col("transacoes_24h") > 0)
            & (F.col("distancia_km") >= 0)
            & F.col("internacional").isin(0, 1)
            & F.col("fraude").isin(0, 1)
        ),
        "avaliacoes": (
            F.col("texto").isNotNull()
            & (F.length(F.trim(F.col("texto"))) > 0)
            & F.col("sentimento").isin(0, 1)
        ),
        "avaliacoes_produto": (
            (F.col("usuario_id") > 0)
            & (F.col("produto_id") > 0)
            & F.col("nota").between(1, 5)
        ),
    }
    try:
        for nome, regra in regras.items():
            origem = BRONZE / nome
            if not origem.exists():
                raise SystemExit(
                    f"Bronze ausente: {origem}. Rode 01_bronze_ingestao.py."
                )
            bruto = spark.read.parquet(str(origem))
            limpo = bruto.dropDuplicates().filter(
                F.coalesce(regra, F.lit(False))
            )
            destino = SILVER / nome
            limpo.write.mode("overwrite").parquet(str(destino))
            print(
                f"{nome}: {limpo.count()} aprovadas, "
                f"{bruto.count() - limpo.count()} rejeitadas"
            )
        titulo("Silver pronta: dados validados para os modelos")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
