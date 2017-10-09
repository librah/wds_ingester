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
RABBIT_MQ_IN_QUEUE = os.getenv('RABBIT_MQ_IN_QUEUE') if 'RABBIT_MQ_IN_QUEUE' in os.environ else 'w4cs_input_queue'
RABBIT_MQ_OUT_QUEUE = os.getenv('RABBIT_MQ_OUT_QUEUE') if 'RABBIT_MQ_OUT_QUEUE' in os.environ else 'w4cs_output_queue'
RABBIT_MQ_USER = os.getenv('RABBIT_MQ_USER') if 'RABBIT_MQ_USER' in os.environ else None
RABBIT_MQ_PASSWD = os.getenv('RABBIT_MQ_PASSWD') if 'RABBIT_MQ_PASSWD' in os.environ else None


def update_document(doc):
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


def delete_document(id):
    logger.info('Delete document %s', id)
    wds_result = discovery.delete_document(WDS_ENV_ID, WDS_COLLECTION_ID, id)
    logger.info('WDS response: %s', wds_result)
    return wds_result


def get_document(id):
    qopts = {
        'filter': '(id::"%s")' % id
    }
    result = discovery.query(WDS_ENV_ID, WDS_COLLECTION_ID, qopts)
    if result['matching_results'] != 1:
        logger.info('%d matches found for docid %s', result['matching_results'], id)
        return None
    else:
        return result['results'][0]


def callback(ch, method, properties, body):
    try:
        request = json.loads(body, encoding='utf-8')

        method = request['method']
        body = request['body']
        if method == 'update':
            wds_result = update_document(body)
            doc_id = wds_result['document_id']
            time.sleep(1)
            while True:
                doc_info = discovery.get_document(WDS_ENV_ID, WDS_COLLECTION_ID, doc_id)
                logger.info('Checking document %s status: %s', doc_id, doc_info)
                doc_status = doc_info['status']
                if doc_status == 'available' or doc_status == 'available with notices':
                    logger.info('Pull the enriched result from WDS and put to output queue')
                    enriched_doc = get_document(doc_id)
                    mq_out_channel.basic_publish(exchange='', routing_key=RABBIT_MQ_OUT_QUEUE, body=enriched_doc, properties=pika.BasicProperties(delivery_mode=2))
                    break
                elif doc_status == 'processing':
                    time.sleep(5)
                    continue
                else:
                    raise 'Unexpected get_document status error'
        elif method == 'delete':
            delete_document(body)
            mq_out_channel.basic_publish(exchange='', routing_key=RABBIT_MQ_OUT_QUEUE, body=request, properties=pika.BasicProperties(delivery_mode=2))
        else:
            raise 'Unknown operation %s' % method
    except:
        logger.exception('Unabe to process received document, move document to error queue: %s', body)
    finally:
        mq_in_channel.basic_ack(delivery_tag=method.delivery_tag)


#
# Main program
#
if __name__ == '__main__':
    collection = discovery.get_collection(WDS_ENV_ID, WDS_COLLECTION_ID)
    logger.info('Ingesting documents to WDS collection: %s (%s)', collection['name'], collection['collection_id'])

    mq_credential = pika.PlainCredentials(RABBIT_MQ_USER, RABBIT_MQ_PASSWD)
    mq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_MQ_HOST, credentials=mq_credential))

    # create channel for putting output
    mq_out_channel = mq_conn.channel()
    mq_out_channel.queue_declare(queue=RABBIT_MQ_OUT_QUEUE, durable=True)

    # create channel for receiving input
    mq_in_channel = mq_conn.channel()

    mq_in_channel.queue_declare(queue=RABBIT_MQ_IN_QUEUE, durable=True)
    mq_in_channel.basic_qos(prefetch_count=1)
    mq_in_channel.basic_consume(callback, queue=RABBIT_MQ_IN_QUEUE)

    logger.info('Monitoring documents from %s at %s', RABBIT_MQ_IN_QUEUE, RABBIT_MQ_HOST)
    mq_in_channel.start_consuming()
