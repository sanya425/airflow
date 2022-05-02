# airflow


    #init_vault
    
    docker exec -it VAULT_DOCKER_ID sh

    vault login ZyrP7NtNw0hbLUqu7N3IlTdO

    vault secrets enable -path=airflow -version=2 kv

    vault kv put airflow/variables/slack_token value=T03D2PKRWAU/B03CD1SNQAE/dkEprnaNsutaZmRn1F1g5oIk
    