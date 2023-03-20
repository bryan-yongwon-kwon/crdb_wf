from cred_type import CredType
from cred_name import CredName
from storage_workflows.crdb.aws.account_type import AccountType
from storage_workflows.crdb.aws.secret_manager import SecretManager
import os
import stat


class CrdbCredential:
    _CERTS_DIR_PATH_PREFIX = "/app/crdb/certs"

    def __init__(self, account_type: AccountType, cluster_name:str, cred_type: CredType):
        self._account_type = account_type
        self._cluster_name = cluster_name
        self._cred_type = cred_type
        self._secret_manager = SecretManager(account_type, cluster_name)

    def _get_cred_file_name(self):
        match self._cred_type:
            case CredType.CA_CERT_CRED_TYPE:
                return CredName.CA_CERT_FILE_NAME.value
            case CredType.PUBLIC_CERT_CRED_TYPE:
                return CredName.ROOT_PUBLIC_CERT_FILE_NAME.value
            case CredType.PRIVATE_KEY_CRED_TYPE:
                return CredName.ROOT_PRIVATE_KEY_FILE_NAME.value
            case _:
                return ""

    def _get_certs_dir_path(self) -> str:
        return self._CERTS_DIR_PATH_PREFIX + "/" + self._cluster_name + "/"
    
    def _write_to_file(self, file_name:str, content:str):
        dir_path = self._get_certs_dir_path() + file_name
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = dir_path + file_name
        file = open(file_path, "w")
        file.write(content)
        file.close()
        os.chmod(file_path, stat.S_IREAD|stat.S_IWRITE)

    def write_certs_into_filesystem(self):
        secret_manager = SecretManager(self._account_type, self._cluster_name)
        self._write_to_file(self._get_cred_file_name(), secret_manager.get_crdb_ca_cert())

    def get_credential_path(self) -> str:
        return self._get_certs_dir_path() + self._get_cred_file_name()
