# Pipeline base on airflow

##Installation

1) Run in the terminal
```bash
mkdir ./logs
```
2) Initialize the database
```bash
docker-compose up airflow-init
```
3) Running Airflow
```bash
docker-compose up 
```
4) Initialize secret backend storage - Vault
```bash
#find vault container id
docker ps

docker exec -it VAULT_DOCKER_ID sh

vault login ZyrP7NtNw0hbLUqu7N3IlTdO

vault secrets enable -path=airflow -version=2 kv

vault kv put airflow/variables/slack_token value=T03D2PKRWAU/B03CD1SNQAE/dkEprnaNsutaZmRn1F1g5oIk
```

## Usage
1) Create file if not exist
```bash
touch plugins/run.txt
```
2) Follow the link http://localhost:8080/

3) Turn on DAGs with name "trigger_dag", "dag_id_2" and wait 
    