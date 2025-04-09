# How to create docker image and push it on Azure

## Compile and test locally the container
Step 1: compile using the following commands:
> docker build -t tor-proxy .
> docker run -d -p 9050:9050 --name tor-proxy tor-proxy

Step 2: execute and test the container:
> docker run -d --name tor-proxy -p 9050:9050 tor-proxy
> curl --proxy socks5h://localhost:9050 https://check.torproject.org


## Publish on Azure Container Registry (ACR)
Step 1: create and login into an ACR:
> az acr create --resource-group <RESOURCE_GROUP> --name <ACR_NAME> --sku Basic
> az acr login --name <ACR_NAME>

Step 2: tag and push the image into the registry:
> ACR_LOGIN_SERVER=$(az acr show --name <ACR_NAME> --query loginServer --output tsv)
> docker tag tor-proxy $ACR_LOGIN_SERVER/tor-proxy:latest
> docker push $ACR_LOGIN_SERVER/tor-proxy:latest


## Create and test the Container-App on Azure
Step 1: create a vnet and a subnet in order to enable TCP connections:
> az network vnet create --name <VNET_NAME> --resource-group <RESOURCE_GROUP> --location northeurope --address-prefixes 10.1.0.0/16
> az network vnet subnet create --name <SUBNET_NAME> --resource-group <RESOURCE_GROUP>  --vnet-name <VNET_NAME> --address-prefixes 10.1.0.0/24 --delegations Microsoft.App/environments
> SUBNET_ID=$(az network vnet subnet show --resource-group <RESOURCE_GROUP> --vnet-name <VNET_NAME> --name <SUBNET_NAME> --query "id" -o tsv)

Step 2: create a container-app env:
> az containerapp env create --name <ENV_NAME> --resource-group <RESOURCE_GROUP> --location <REGION> --infrastructure-subnet-resource-id $SUBNET_ID

Step 3: create the container app with Tor image:
> az containerapp create \
  --name tor-proxy \
  --resource-group <RESOURCE_GROUP> \
  --environment <ENV_NAME> \
  --image $ACR_LOGIN_SERVER/tor-proxy:latest \
  --cpu 0.5 --memory 1Gi \
  --registry-server $ACR_LOGIN_SERVER \
  --registry-username <ACR_USERNAME> \ 
  --registry-password <ACR_PASSWORD> \
  --target-port 9050 \
  --ingress external \
  --transport tcp \
  --query properties.configuration.ingress.fqdn

Step 4: test your deploy:
> tor-proxy.<other_stuff>.<your-region>.azurecontainerapps.io
> curl --proxy socks5h://tor-proxy.<other_stuff>.<your-region>.azurecontainerapps.io:9050 https://check.torproject.org

Step 5: configure autoscaling
> az containerapp revision set-mode \
  --name tor-proxy \
  --resource-group <RESOURCE_GROUP> \
  --mode single
> az containerapp update \
  --name tor-proxy \
  --resource-group <RESOURCE_GROUP> \
  --min-replicas 0 \
  --max-replicas 3

# TODO: Ruota l'IP periodicamente riavviando il container per ottenere un nuovo circuito Tor.

