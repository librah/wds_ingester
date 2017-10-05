import json
import logging
import os
from StringIO import StringIO
import sys
import time

import pika

from watson_developer_cloud import DiscoveryV1


#
#  Set the default logging
#
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ingester')


#
# Checking the required environment variables and initialize them
#
for required_env in ('WDS_USER', 'WDS_PASSWD', 'WDS_COLLECTION_ID', 'WDS_ENV_ID', 'WDS_VERSION'):
    if required_env not in os.environ:
        print 'Missing required env variable "%s"' % required_env
        sys.exit(-1)

WDS_COLLECTION_ID = os.getenv('WDS_COLLECTION_ID')
WDS_ENV_ID = os.getenv('WDS_ENV_ID')

discovery_option = dict()
discovery_option['username'] = os.getenv('WDS_USER')
discovery_option['password'] = os.getenv('WDS_PASSWD')
if 'WDS_URL' in os.environ:
    discovery_option['url'] = os.getenv('WDS_URL')


#
# Creating the WDS wrapper
#
discovery = DiscoveryV1(os.getenv('WDS_VERSION'), **discovery_option)


#
# Creating the RabbitMQ wrapper
#
RABBIT_MQ_HOST = os.getenv('RABBIT_MQ_HOST') if 'RABBIT_MQ_HOST' in os.environ else 'rabbitmq'
RABBIT_MQ_QUEUE = os.getenv('RABBIT_MQ_QUEUE') if 'RABBIT_MQ_QUEUE' in os.environ else 'task_queue'
RABBIT_MQ_USER = os.getenv('RABBIT_MQ_USER') if 'RABBIT_MQ_USER' in os.environ else None
RABBIT_MQ_PASSWD = os.getenv('RABBIT_MQ_PASSWD') if 'RABBIT_MQ_PASSWD' in os.environ else None


def ingest_document(doc):
    io = StringIO()
    json.dump(doc, io)
    io.seek(0)
    if 'id' in doc:
        logger.info('Receive document and ingesting %s', doc['id'])
        wds_result = discovery.update_document(WDS_ENV_ID, WDS_COLLECTION_ID, doc['DOCNO'], file_data=io, mime_type='application/json')
    else:
        logger.info('Receive document and ingesting')
        wds_result = discovery.add_document(WDS_ENV_ID, WDS_COLLECTION_ID, file_data=io, mime_type='application/json')
    logger.info('WDS response: %s', wds_result)
    return wds_result


def callback(ch, method, properties, body):
    try:
        doc = json.loads(body, encoding='utf-8')
        wds_result = ingest_document(doc)
        doc_id = wds_result['document_id']
        time.sleep(1)
        while True:
            doc_info = discovery.get_document(WDS_ENV_ID, WDS_COLLECTION_ID, doc_id)
            logger.info('Checking document %s status: %s', doc_id, doc_info)
            doc_status = doc_info['status']
            if doc_status == 'available' or doc_status == 'available with notices':
                logger.info('Pull the enriched result from WDS and put to output queue')
                break
            elif doc_status == 'processing':
                time.sleep(5)
                continue
            else:
                raise 'Unexpected get_document status error'
    except:
        logger.exception('Unabe to process received document, move document to error queue: %s', body)
    finally:
        mq_channel.basic_ack(delivery_tag=method.delivery_tag)


#
# Main program
#
if __name__ == '__main__':
    collection = discovery.get_collection(WDS_ENV_ID, WDS_COLLECTION_ID)
    logger.info('Ingesting documents to WDS collection: %s (%s)', collection['name'], collection['collection_id'])

    mq_credential = pika.PlainCredentials(RABBIT_MQ_USER, RABBIT_MQ_PASSWD)
    mq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_MQ_HOST, credentials=mq_credential))
    mq_channel = mq_conn.channel()

    mq_channel.queue_declare(queue=RABBIT_MQ_QUEUE, durable=True)
    mq_channel.basic_qos(prefetch_count=1)
    mq_channel.basic_consume(callback, queue=RABBIT_MQ_QUEUE)

    logger.info('Monitoring documents from %s at %s', RABBIT_MQ_QUEUE, RABBIT_MQ_HOST)
    mq_channel.start_consuming()
