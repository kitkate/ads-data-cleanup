# MLOps Engineer challenge

## Setup the project

### Prerequisites

* Sign in to Azure with an Azure user that has Contributor rights

### Setup Databricks Workspace

* Go to terraform-databricks and run
```
    terraform init
    terraform apply
```
As an output you will get the url to the newly created databricks account

### Setup Data Lake with the supplied data

* From the Azure portal create new Storage account - from the Advanced tab choose *enable hierarchical namespace* to be enabled
* Create Container
* Upload ads.json and views.json to the container (eg via [Microsoft Azure Storage Exlorer](https://learn.microsoft.com/en-us/azure/architecture/data-science-process/move-data-to-azure-blob-using-azure-storage-explorer))

### Run Solution notebook

* Create a compute
* In the first node of the notebook there is an instruction how to connect it to the data lake. Set it up.
* Run the notebook
* In the run_at_noon.yaml you can find the YAML for the Job that runs every weekday at 12:00
