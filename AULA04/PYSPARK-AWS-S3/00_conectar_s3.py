"""Primeiro contato com o S3 -- sem Spark ainda.

    python 00_conectar_s3.py

Antes de montar um ETL que le e grava no S3, vale ver as quatro operacoes
que ele oferece, isoladas: conectar, subir, listar, baixar. Sao as mesmas
quatro que o `01_etl_energia_sc.py` inteiro usa, so que la escondidas
dentro de funcoes maiores.

O floci (veja o SETUP.md) precisa estar rodando antes deste script:

    docker compose up -d --wait

Este script nao le nem grava nada em disco fora de um arquivo temporario --
ele existe para provar que a conexao funciona antes de confiar nela dentro
de um pipeline maior.
"""

from botocore.exceptions import ClientError

from comum import BUCKET, obter_bucket, titulo

titulo("1. Conectar e garantir que o bucket existe")
s3 = obter_bucket()
print(f"conectado ao bucket '{BUCKET}'")

titulo("2. Subir um objeto pequeno")
conteudo = b"Se voce esta lendo isto de volta, o upload e o download funcionaram.\n"
s3.put_object(Bucket=BUCKET, Key="teste/ola.txt", Body=conteudo)
print("objeto enviado: teste/ola.txt")

titulo("3. Listar os objetos do bucket")
# `list_objects_v2` devolve no maximo 1000 chaves por chamada -- para mais
# que isso, existe paginacao (`ContinuationToken`), sem uso aqui.
resposta = s3.list_objects_v2(Bucket=BUCKET, Prefix="teste/")
for objeto in resposta.get("Contents", []):
    print(f"  {objeto['Key']}  ({objeto['Size']} bytes)")

titulo("4. Baixar o objeto e conferir o conteudo")
baixado = s3.get_object(Bucket=BUCKET, Key="teste/ola.txt")["Body"].read()
print(baixado.decode("utf-8").strip())
assert baixado == conteudo, "o conteudo baixado nao bate com o que foi enviado"
print("conteudo conferido: identico ao que foi enviado")

titulo("5. Apagar o objeto de teste")
s3.delete_object(Bucket=BUCKET, Key="teste/ola.txt")
try:
    s3.get_object(Bucket=BUCKET, Key="teste/ola.txt")
    print("ainda esta la -- isso nao deveria acontecer")
except ClientError as erro:
    if erro.response["Error"]["Code"] != "NoSuchKey":
        raise
    print("confirmado: teste/ola.txt nao existe mais")

print("\nConexao com o S3 funcionando. Proximo passo: 01_etl_energia_sc.py")
