"""Configuração de acesso ao Azure Blob do data mart Hotmart.

Mesmo padrão do facebook_config/voomp_config: connection string da conta/chave do
ambiente e containers compartilhados (datamartbi*). Auto-cria o container se faltar.
"""
import os

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

RAW = os.getenv("HOTMART_CONTAINER_RAW", "datamartbiraw")
STAGE = os.getenv("HOTMART_CONTAINER_STAGE", "datamartbistage")
BRONZE = os.getenv("HOTMART_CONTAINER_BRONZE", "datamartbibronze")
SILVER = os.getenv("HOTMART_CONTAINER_SILVER", "datamartbisilver")
GOLD = os.getenv("HOTMART_CONTAINER_GOLD", "datamartbigold")

CONTAINERS = {"raw": RAW, "stage": STAGE, "bronze": BRONZE, "silver": SILVER, "gold": GOLD}


def get_blob_service_client() -> BlobServiceClient:
    cs = (
        f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};"
        f"AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(cs)


def get_container_client(layer: str, create: bool = True) -> ContainerClient:
    if layer not in CONTAINERS:
        raise ValueError(f"Camada desconhecida: {layer!r}. Use uma de {list(CONTAINERS)}.")
    cc = get_blob_service_client().get_container_client(CONTAINERS[layer])
    if create:
        try:
            cc.create_container()
        except ResourceExistsError:
            pass
    return cc
