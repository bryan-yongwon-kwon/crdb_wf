import typer
from storage_workflows.crdb.api_gateway.crdb_connect_gateway import CrdbConnectGateway
from storage_workflows.crdb.api_gateway.aws_client_gateway import AwsClientGateway
from storage_workflows.crdb.aws.deployment_env import DeploymentEnv
from storage_workflows.crdb.operations.health_check import HealthCheck

app = typer.Typer()

@app.command()
def echo(message):
    print(message)

@app.command()
def echo2(message1, message2):
    print(message1+message2)

if __name__ == "__main__":
    app()