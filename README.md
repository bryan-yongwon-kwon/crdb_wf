# storage-workflows
The repo for storage Argo Workflows.

Some useful links:
 - [Operator Service design doc](https://docs.google.com/document/d/1paeLrixuwF9V_8LLMsiT6d3DpEoPo20lDKp9vNoDEv4/edit#heading=h.kmt9462l4emm)
 - [Argo Workflows](https://argo-workflows.infra-control-plane.doordash.red/workflows/storage-workflows?limit=50)
 - [Argo CD](https://argocd.infra-control-plane.doordash.red/) (search `storage-argo-workflows`)
 - storage [workflow templates](https://github.com/doordash/cluster-config/tree/master/argocd/workflows/storage) (today we are using `storage-workflows-test` branch for development)

### What's in this repo

##### build.yml and ci.yml
`build.yml` defines where to upload the docker image we build. `ci.yml` defines the ci pipeline with stages. We run unit tests in ci pipeline. For more information please read [YML Pipelines](https://doordash.atlassian.net/wiki/spaces/EJ/pages/1040121920/YML+Pipelines).
##### Dockerfile
Every time we commit to this repo, the CI/CD pipeline will build a docker image and upload to our AWS ECR repo(called `storage-workflows`). This dockerfile contains the basic softwares we need and ENV variables. 

##### poetry
In our python project, we use [poetry](https://python-poetry.org/) as the dependency management tool. The dependencies can be found in `pyproject.toml` config file. Note that `poetry.lock` file is generated, please do not modified it manually.

##### typer
We also use [typer](https://typer.tiangolo.com/) to help us build the python cli. In Argo Workflows, our python code is called from template's bash script. So having a cli tool will make the integration easier. Check main.py for examples.

##### storage_workflows
This is the place to implement the business logic of each step. Check `storage_workflows/crdb` for examples(AWS integration, CRDB SQL query, etc.). 
If you want to use the operator service for your service, you should create a sub directory for your own service like `storage_workflows/YOUR_SERVICE`.

### How to develop
##### Run
In local environment, go to `storage_workflows` directory and run `python main.py echo2 a b`.
If you are seeing module not found issue, you might need to temporarily add this to main.py's import section to run code locally.
```
import sys
sys.path.append("/Users/aochen/Projects/storage-workflows") # replace with your own path
```

In Docker container, just run `/usr/local/bin/storage-workflows echo2 a b`.
##### Unit Test
To run unit tests, use ```poetry run pytest```.
For more documentation, please refer to [pytest](https://docs.pytest.org/en/7.2.x/).

##### Test in Argo Workflows
To test in Argo Workflows:
1. Raise a pr. Your docker image will be uploaded to AWS ECR if all the checks pass.
2. Go to AWS ECR and find your image in `storage-workflows`. Copy the URI.
3. Got to cluster-config repo. Checkout `storage-workflows-test` branch. Add your template and specify the docker image URI to use it. Commit you changes.
4. Go to [Argo CD](https://argocd.infra-control-plane.doordash.red/). Make sure it's in sync. If not, you can click `sync` button to trigger sync.
5.  Then you should be able to find your template in [Argo Workflows](https://argo-workflows.infra-control-plane.doordash.red/workflows/storage-workflows?limit=50).
6.  If you see errors, you can check `Containers` -> `Logs` to view logs.