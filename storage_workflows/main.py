import typer
from storage_workflows.setup_env import setup_env

app = typer.Typer()

@app.command()
def echo(message):
    print(message)

@app.command()
def echo2(message1, message2):
    print(message1+message2)

if __name__ == "__main__":
    app()