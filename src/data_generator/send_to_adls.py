from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
import sys

load_dotenv()
connection_string = os.getenv("ADLS_CONN_STR")
tables = ['orders', 'order_items', 'order_payments']
container_name = 'olist'

# Exemplo de uso
def list_blobs_in_directory(container_name: str, directory_path: str):
    """
    Lista os blobs em um 'diretório' dentro de um container do Azure Blob Storage.

    :param container_name: Nome do container no Blob Storage
    :param directory_path: Caminho do 'diretório' no Blob Storage (prefixo)
    :param connection_string: String de conexão do Azure Blob Storage
    :return: Lista de blobs encontrados no diretório
    """
    
    # Inicializa o BlobServiceClient a partir da string de conexão
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Acessa o container
    container_client = blob_service_client.get_container_client(container_name)

    # Lista os blobs com base no prefixo (caminho do diretório)
    blob_list = container_client.list_blobs(name_starts_with=directory_path)

    blobs = []
    for blob in blob_list:
        if blob.name.endswith('.parquet'):
            blobs.append(blob.name.split('/')[-1])

    return blobs


def upload_parquet_to_blob(table_name: str, filename: str, container_name: str):
    """
    Faz o upload de um arquivo Parquet para o Azure Blob Storage.
    """
    
    # Concatena o caminho completo no sistema local
    local_file_path = f'./data/cdc/{table_name}/{filename}'

    # Inicializa o BlobServiceClient a partir da string de conexão
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Acessa o container
    container_client = blob_service_client.get_container_client(container_name)

    # Define o caminho do blob como igual ao caminho local
    blob_path = f'/raw/cdc/{table_name}/{filename}'  # Se quiser manter subpastas, ajuste aqui

    # Carrega o arquivo local
    with open(local_file_path, "rb") as data:
        # Faz o upload do arquivo para o Azure Blob Storage
        container_client.upload_blob(name=blob_path, data=data, overwrite=True)

    print(f"Arquivo {filename} carregado com sucesso no Blob Storage.")
    
def find_next_cdc():
    """
    Encontra o primeiro arquivo na lista files_cdc que não está na lista blobs.

    :param files_cdc: Lista de arquivos no sistema local
    :param blobs: Lista de blobs no Azure Blob Storage
    :return: O primeiro arquivo que não está na lista de blobs, ou None se todos os arquivos estiverem presentes
    """
    files_cdc = os.listdir('./data/cdc/order_items')
    files_cdc.sort()

    blobs = list_blobs_in_directory(container_name, f'/raw/cdc/order_items')
    blobs.sort()
    for file in files_cdc:
        if file not in blobs:
            return file  # Retorna o primeiro arquivo que não está na lista de blobs
    return None  # Retorna None se todos os arquivos estiverem na lista de blobs


def clear_blobs(container_name: str, directory: str):
    """
    Apaga todos os arquivos Parquet no diretório especificado dentro do container do Azure Blob Storage.
    """

    # Inicializa o BlobServiceClient a partir da string de conexão
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Acessa o container
    container_client = blob_service_client.get_container_client(container_name)

    # Lista todos os blobs no diretório especificado
    blobs_to_delete = container_client.list_blobs(name_starts_with=directory)

    # Filtra e apaga os blobs com extensão '.parquet'
    for blob in blobs_to_delete:
        if blob.name.endswith(".parquet"):
            container_client.delete_blob(blob.name)
            print(f"Blob {blob.name} apagado.")

    print("Todos os arquivos Parquet foram apagados.")


def send_next_cdc():
    cdc = find_next_cdc()

    if cdc:
        print(f"O primeiro arquivo ausente no Blob Storage é: {missing_file}")
        container_name = "olist"
        
        for table_name in tables:
            upload_parquet_to_blob(table_name, cdc, container_name)
        
    else:
        print("Todos os arquivos estão presentes no Blob Storage.")

send_next_cdc()