from enum import Enum


class CredType(Enum):
    PUBLIC_CERT_CRED_TYPE = "client-public-cert"
    PRIVATE_KEY_CRED_TYPE = "client-private-key"
    CA_CERT_CRED_TYPE = "ca-public-certificate"