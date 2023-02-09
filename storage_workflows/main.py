import os
import typer

app = typer.Typer()

@app.command()
def echo(message):
    print(message)

@app.command()
def echo2(message1, message2):
    print(message1+message2)