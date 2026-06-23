"""Configuração central de acesso ao Azure Blob Storage do data mart Voomp.

Centraliza a connection string, os nomes dos containers (com override por
variável de ambiente) e a criação automática de container ("cria se não
existir"), eliminando a duplicação que existia no cabeçalho de cada script.

Uso nos scripts:
    from voomp_config import get_container_client, SILVER as CONTAINER_SILVER
    silver_client = get_container_client("silver")
"""
import os

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv

load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# Novos containers do data mart. Podem ser sobrescritos por variável de ambiente.
# RAW (datamartbiraw): zona crua append-only (snapshot do Excel por ingestion_date).
# STAGE segue como INBOX de chegada do Excel (onde o arquivo é dropado).
RAW = os.getenv("VOOMP_CONTAINER_RAW", "datamartbiraw")
STAGE = os.getenv("VOOMP_CONTAINER_STAGE", "datamartbistage")
BRONZE = os.getenv("VOOMP_CONTAINER_BRONZE", "datamartbibronze")
SILVER = os.getenv("VOOMP_CONTAINER_SILVER", "datamartbisilver")
GOLD = os.getenv("VOOMP_CONTAINER_GOLD", "datamartbigold")

CONTAINERS = {"raw": RAW, "stage": STAGE, "bronze": BRONZE, "silver": SILVER, "gold": GOLD}


def get_blob_service_client() -> BlobServiceClient:
    """Cria o BlobServiceClient a partir da conta/chave do ambiente."""
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT_NAME};"
        f"AccountKey={STORAGE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def get_container_client(layer: str, create: bool = True) -> ContainerClient:
    """Retorna o ContainerClient da camada ('bronze' | 'silver' | 'gold').

    Quando create=True (padrão), garante que o container exista, criando-o
    caso ainda não exista (idempotente).
    """
    if layer not in CONTAINERS:
        raise ValueError(
            f"Camada desconhecida: {layer!r}. Use uma de {list(CONTAINERS)}."
        )
    container_client = get_blob_service_client().get_container_client(CONTAINERS[layer])
    if create:
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass
    return container_client
