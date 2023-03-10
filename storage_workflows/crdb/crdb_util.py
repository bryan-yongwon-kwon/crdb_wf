import os
import stat
import psycopg2
from common_enums import CredType
from common_enums import Env
from aws_util import get_secret
from aws_util import get_aws_client_local

HOST_SUFFIX = "-crdb.us-west-2.aws.ddnw.net"
ROOT = "root"
SECRET_MANAGER = "secretsmanager"
CA_CERT_FILE_NAME = "ca.crt"
ROOT_PUBLIC_FILE_NAME = "client.root.crt"
ROOT_PRIVATE_FILE_NAME = "client.root.key"
PORT = "26257"
SSL_MODE = "require"

def get_crdb_creds(env:Env, cluster_name:str) -> dict:
    secrets_aws_client = get_aws_client_local(env, SECRET_MANAGER)
    private_key = get_secret(secrets_aws_client, cluster_name, env, CredType.PRIVATE_KEY_CRED_TYPE, ROOT)
    public_cert = get_secret(secrets_aws_client, cluster_name, env, CredType.PUBLIC_CERT_CRED_TYPE, ROOT)
    ca_cert = get_secret(secrets_aws_client, cluster_name, env, CredType.CA_CERT_CRED_TYPE)
    return {
        CredType.PRIVATE_KEY_CRED_TYPE.value: private_key,
        CredType.PUBLIC_CERT_CRED_TYPE.value: public_cert,
        CredType.CA_CERT_CRED_TYPE.value: ca_cert
    }

def write_to_file(file_name:str, content:str):
    file = open(file_name, "w")
    file.write(content)
    file.close()
    os.chmod(file_name, stat.S_IREAD|stat.S_IWRITE)

def connect_crdb(env:Env, cluster_name:str, db_name:str):
    crdb_creds = get_crdb_creds(env, cluster_name)
    write_to_file(CA_CERT_FILE_NAME, crdb_creds[CredType.CA_CERT_CRED_TYPE.value])
    write_to_file(ROOT_PUBLIC_FILE_NAME, crdb_creds[CredType.PUBLIC_CERT_CRED_TYPE.value])
    write_to_file(ROOT_PRIVATE_FILE_NAME, crdb_creds[CredType.PRIVATE_KEY_CRED_TYPE.value])
    connection = psycopg2.connect(
        dbname=db_name,
        port=PORT,
        user=ROOT,
        host=cluster_name.replace('_', '-') + HOST_SUFFIX,
        sslmode=SSL_MODE,
        sslrootcert=CA_CERT_FILE_NAME,
        sslcert=ROOT_PUBLIC_FILE_NAME,
        sslkey=ROOT_PRIVATE_FILE_NAME
    )
    return connection
