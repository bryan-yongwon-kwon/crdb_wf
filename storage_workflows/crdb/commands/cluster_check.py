import typer
from storage_workflows.setup_env import setup_env
from storage_workflows.crdb.connect.ssh import SSH

app = typer.Typer()

@app.command()
def check_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region)
    ssh_client = SSH()
    ssh_client.connect_to_node("10.4.112.109") #TODO: replace with nodes ip iteration
    stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")


if __name__ == "__main__":
    app()