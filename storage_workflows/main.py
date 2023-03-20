import typer
from storage_workflows.crdb.aws.secret_manager import SecretManager
from storage_workflows.crdb.aws.account_type import AccountType

app = typer.Typer()

@app.command()
def echo(message):
    print(message)

@app.command()
def echo2(message1, message2):
    print(message1+message2)

@app.command()
def testAWS():
    secret_manager = SecretManager(AccountType.STAGING, "ao_test_backup")
    cert = secret_manager.get_crdb_ca_cert()
    print(cert)

if __name__ == "__main__":
    app()