# IBM Watson Discovery Service (WDS) - Document Ingester
WDS Ingester read documents from the input queue, ingest the documents to IBM WDS,
and put the WDS enriched response documents to the response queue.


## Run Rabbit MQ
```console
$ docker run -d --hostname my-rabbit --name my-rabbit -p 15672:15672 -e RABBITMQ_DEFAULT_USER=<user> -e RABBITMQ_DEFAULT_PASS=<password> rabbitmq:3-management
```

Visit the admin interface at: `http://my-rabbit:15672`

## Run WDS Ingester
```console
$ docker run -d --rm --link=my-rabbit:rabbitmq -e WDS_USER=<username> -e WDS_PASSWD=<password> -e WDS_COLLECTION_ID=<collection_id> -e WDS_ENV_ID=<env_id> -e WDS_VERSION=<version> -e RABBIT_MQ_USER=<user> -e RABBIT_MQ_PASSWD=<passwd> --name my-ingester librah/wds_ingester 
```

