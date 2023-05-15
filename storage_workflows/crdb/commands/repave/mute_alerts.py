import typer
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway

app = typer.Typer()

@app.command()
def mute_alerts_repave(cluster_name):    
    label_matchers = [
        {
            "name": "cluster",
            "type": "EXACT",
            "value": cluster_name
        },
        {
            "name": "Description",
            "type": "EXACT",
            "value": "The count of live nodes has decreased"
        },
        {
            "name": "Description",
            "type": "EXACT",
            "value": "Changefeed is Stopped"
        },
        {
            "name": "Description",
            "type": "EXACT",
            "value": "Underreplicated Range Detected"
        },
        {
            "name": "Description",
            "type": "EXACT",
            "value": "Incremental or full backup failed."
        },
    ]
    
    client = ChronosphereApiGateway()
    client.create_muting_rule(label_matchers)


if __name__ == "__main__":
    app()