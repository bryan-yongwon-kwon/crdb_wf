import datetime
from storage_workflows.crdb.chronsphere.chronosphere_client import ChronosphereClient

app = typer.Typer()

@app.command()
def create_alert_muting_rule(cluster_name):
    # Define the client and muting rule parameters
    client = ChronosphereClient()
    label_matchers = [
        {
            "name": "cluster",
            "type": "EXACT",
            "value": cluster_name
        }
    ]
    name = "Muting rule created from operator service for " + cluster_name
    start_time = datetime.datetime.now(datetime.timezone.utc)
    end_time = start_time + datetime.timedelta(hours=1)
    path = "/api/v1/config/muting-rules"
    http_method = "POST"

    # Create the muting rule
    client.create_muting_rule(label_matchers, name, start_time, end_time, path=path, http_method=http_method)

if __name__ == "__main__":
    app()
