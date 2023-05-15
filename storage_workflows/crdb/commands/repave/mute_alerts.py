import typer
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway

app = typer.Typer()

@app.command()
def mute_alerts_repave(cluster_name):    
    cluster_name_label_matcher = {
            "name": "cluster",
            "type": "EXACT",
            "value": cluster_name
        }
    
    live_node_count_changed_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "The count of live nodes has decreased"
        }

    changefeed_stoppped_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "Changefeed is Stopped"
        }
    
    underreplicated_range_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "Underreplicated Range Detected"
        }
    
    backup_failed_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "Incremental or full backup failed."
        }
    
    mute_alerts_using_label_matchers([cluster_name_label_matcher, live_node_count_changed_label_matcher])
    mute_alerts_using_label_matchers([cluster_name_label_matcher, changefeed_stoppped_label_matcher])
    mute_alerts_using_label_matchers([cluster_name_label_matcher, underreplicated_range_label_matcher])
    mute_alerts_using_label_matchers([cluster_name_label_matcher, backup_failed_label_matcher])

def mute_alerts_using_label_matchers(label_matchers):
    client = ChronosphereApiGateway()
    client.create_muting_rule(label_matchers)

if __name__ == "__main__":
    app()