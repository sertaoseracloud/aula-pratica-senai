"""Gera fontes sinteticas e as carrega sem limpeza na Bronze."""

import csv
import math
import random

from comum import BRONZE, DADOS, criar_sessao, titulo


def gerar_fontes() -> None:
    DADOS.mkdir(parents=True, exist_ok=True)

    fraude_path = DADOS / "transacoes.csv"
    if not fraude_path.exists():
        rng = random.Random(411)
        with fraude_path.open("w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([
                "transacao_id", "valor", "hora", "transacoes_24h",
                "distancia_km", "internacional", "fraude",
            ])
            for transacao_id in range(1, 1801):
                valor = round(min(rng.expovariate(1 / 85), 1500), 2)
                hora = rng.randint(0, 23)
                quantidade = rng.randint(1, 10)
                distancia = round(rng.uniform(0, 500), 1)
                internacional = int(rng.random() < 0.08)
                risco = -5.0 + valor / 260
                risco += 0.9 if hora <= 4 or hora >= 23 else 0
                risco += 0.35 * max(0, quantidade - 5)
                risco += 1.1 if distancia > 250 else 0
                risco += 0.8 * internacional
                chance = 1 / (1 + math.exp(-risco))
                fraude = int(rng.random() < chance)
                writer.writerow([
                    transacao_id, valor, hora, quantidade, distancia,
                    internacional, fraude,
                ])

    sentimento_path = DADOS / "avaliacoes.csv"
    if not sentimento_path.exists():
        rng = random.Random(412)
        positivas = [
            "produto excelente entrega rapida recomendo",
            "qualidade otima chegou perfeito gostei muito",
            "funciona bem compra maravilhosa valeu cada centavo",
            "atendimento excelente produto acima da expectativa",
            "muito satisfeito entrega no prazo e produto impecavel",
        ]
        negativas = [
            "produto pessimo entrega atrasada nao recomendo",
            "qualidade ruim chegou quebrado fiquei decepcionado",
            "nao funciona compra horrivel dinheiro perdido",
            "atendimento ruim produto abaixo da expectativa",
            "muito insatisfeito atrasou e veio com defeito",
        ]
        with sentimento_path.open(
            "w", newline="", encoding="utf-8"
        ) as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow(["avaliacao_id", "texto", "sentimento"])
            for avaliacao_id in range(1, 1201):
                sentimento = int(rng.random() >= 0.5)
                frases = positivas if sentimento else negativas
                texto = rng.choice(frases)
                texto += f" pedido numero {avaliacao_id}"
                writer.writerow([avaliacao_id, texto, sentimento])

    avaliacoes_path = DADOS / "avaliacoes_produto.csv"
    if not avaliacoes_path.exists():
        rng = random.Random(413)
        with avaliacoes_path.open(
            "w", newline="", encoding="utf-8"
        ) as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow(["usuario_id", "produto_id", "nota"])
            produto_por_grupo = {
                grupo: list(range(grupo * 10 + 1, grupo * 10 + 11))
                for grupo in range(4)
            }
            for usuario_id in range(1, 81):
                grupo_preferido = (usuario_id - 1) // 20
                produtos_escolhidos = set(rng.sample(
                    produto_por_grupo[grupo_preferido], 8
                ))
                outros = [
                    produto_id
                    for grupo, produtos in produto_por_grupo.items()
                    if grupo != grupo_preferido
                    for produto_id in produtos
                ]
                produtos_escolhidos.update(rng.sample(outros, 4))
                for produto_id in sorted(produtos_escolhidos):
                    mesmo_grupo = (
                        (produto_id - 1) // 10 == grupo_preferido
                    )
                    nota = (
                        rng.randint(4, 5)
                        if mesmo_grupo
                        else rng.randint(1, 3)
                    )
                    writer.writerow([usuario_id, produto_id, nota])


def main() -> None:
    gerar_fontes()
    spark = criar_sessao("aula06-parte2-01-bronze")
    try:
        fontes = (
            "transacoes", "avaliacoes", "avaliacoes_produto",
        )
        for nome in fontes:
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
