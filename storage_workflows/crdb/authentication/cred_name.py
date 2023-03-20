from enum import Enum

class CredName(Enum):
    CA_CERT_FILE_NAME = "ca.crt"
    ROOT_PUBLIC_CERT_FILE_NAME = "client.root.crt"
    ROOT_PRIVATE_KEY_FILE_NAME = "client.root.key"