"""Primeiro contato com o Azure Blob Storage -- sem Spark ainda.

    python 00_conectar_blob.py

Antes de montar um ETL que le e grava no Blob, vale ver as quatro operacoes
que ele oferece, isoladas: conectar, subir, listar, baixar. Sao as mesmas
quatro que o `01_etl_temperaturas_sc.py` inteiro usa, so que la escondidas
dentro de funcoes maiores.

O floci-az (veja o SETUP.md) precisa estar rodando antes deste script:

    docker compose up -d --wait

Este script nao lê nem grava nada em disco fora de um arquivo temporario --
ele existe para provar que a conexao funciona antes de confiar nela dentro
de um pipeline maior.
"""

from azure.core.exceptions import ResourceNotFoundError

from comum import CONTAINER, obter_container_client, titulo

titulo("1. Conectar e garantir que o container existe")
container = obter_container_client()
print(f"conectado ao container '{CONTAINER}'")

titulo("2. Subir um blob pequeno")
conteudo = b"Se voce esta lendo isto de volta, o upload e o download funcionaram.\n"
container.upload_blob(name="teste/ola.txt", data=conteudo, overwrite=True)
print("blob enviado: teste/ola.txt")

titulo("3. Listar os blobs do container")
# `list_blobs` devolve um iterador -- so bate na rede quando voce consome.
# `name_starts_with` filtra do lado do servidor, sem baixar o que nao interessa.
for blob in container.list_blobs(name_starts_with="teste/"):
    print(f"  {blob.name}  ({blob.size} bytes)")

titulo("4. Baixar o blob e conferir o conteudo")
baixado = container.get_blob_client("teste/ola.txt").download_blob().readall()
print(baixado.decode("utf-8").strip())
assert baixado == conteudo, "o conteudo baixado nao bate com o que foi enviado"
print("conteudo conferido: identico ao que foi enviado")

titulo("5. Apagar o blob de teste")
container.delete_blob("teste/ola.txt")
try:
    container.get_blob_client("teste/ola.txt").download_blob()
    print("ainda esta la -- isso nao deveria acontecer")
except ResourceNotFoundError:
    print("confirmado: teste/ola.txt nao existe mais")

print("\nConexao com o Blob Storage funcionando. Proximo passo: 01_etl_temperaturas_sc.py")
