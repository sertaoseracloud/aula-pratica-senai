"""Primeiro contato com o Azure Blob Storage -- sem Spark ainda.

    python 00_conectar_blob.py

As mesmas cinco operacoes do laboratorio de ETL da AULA04, isoladas: aqui
elas ganham um papel a mais, porque cada camada da arquitetura medalhao vai
usa-las para subir e baixar uma pasta Parquet inteira, nao so um arquivo.

O floci-az (veja o SETUP.md) precisa estar rodando antes deste script:

    docker compose up -d --wait
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

print("\nConexao com o Blob Storage funcionando. Proximo passo: 01_bronze_ingestao.py")
