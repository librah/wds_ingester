# IBM Watson Discovery Service (WDS) - Document Ingester
WDS Ingester read documents from `input`, ingest the documents to IBM WDS,
and put the WDS enriched response documents to `output`.

Currently, the `input` and `output` can be a directory.
In the future, `input` and `output` can be a RabbitMQ.

## Run WDS Ingester


```console
$ 
$ docker run -d --rm -e WDS_USER=<username> -e WDS_PASSWD=<password> -e WDS_COLLECTION_ID=<collection_id> -e WDS_ENV_ID=<env_id> -e WDS_VERSION=<version> -e RABBIT_MQ_USER=<user> -e RABBIT_MQ_PASSWD=<passwd> --name my-ingester librah/wds_ingester 
```

