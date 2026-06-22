"""Configuração central de acesso ao Azure Blob Storage do data mart Facebook.

Mesmo padrão do include/voomp/voomp_config.py: connection string, nomes dos
containers (com override por env) e auto-criação de container ("cria se não
existir"). Os containers são compartilhados entre os data marts (datamartbi*).

Uso nos scripts:
    from facebook_config import get_container_client
    bronze_client = get_container_client("bronze")
"""
import os

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv

load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# Containers do data mart (mesmos do voomp). Override por variável de ambiente.
BRONZE = os.getenv("FB_CONTAINER_BRONZE", "datamartbibronze")
SILVER = os.getenv("FB_CONTAINER_SILVER", "datamartbisilver")
GOLD = os.getenv("FB_CONTAINER_GOLD", "datamartbigold")

CONTAINERS = {"bronze": BRONZE, "silver": SILVER, "gold": GOLD}


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
