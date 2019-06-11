import argparse
import codecs
import json
import logging
import multiprocessing
import os
import sys
import time
from queue import PriorityQueue
from io import StringIO
from threading import Thread
import threading

from ibm_watson import DiscoveryV1

# Use the built-in version of scandir/walk if possible, otherwise
# use the scandir module version
try:
    from os import scandir
except ImportError:
    from scandir import scandir

#
#  Set the default logging
#
logger = logging.getLogger('ingester')

#
# The threads in this code
#
thread_pool = []  # all the threads (including producer & consumer) run in this code

# Infinite queue to store the file change events (priority high to low)
#  - 0: poison pill (for stopping threads)
#  - 1: documents to be updated/deleted
request_queue = PriorityQueue(1000)
CONST_Q_PRIORITY_POISON = 0
CONST_Q_PRIORITY_UPDATE = 1


class WDSIngester:
    def __init__(self, user, password, env_id, collection_id, version, processing_cap=10):
        discovery_option = {
            'username': user,
            'password': password
        }
        self.discovery = DiscoveryV1(version, **discovery_option)
        self.env_id = env_id
        self.collection_id = collection_id
        self.version = version
        self.processing_cap = processing_cap
        if self.processing_cap <= 0:
            self.processing_cap = 10

        collection = self.discovery.get_collection(self.env_id, self.collection_id).get_result()
        logger.info('Creating WDSIngester to WDS collection: %s (%s, cap: %d)', collection['name'], collection['collection_id'], self.processing_cap)

    def update_document(self, doc_id, file_name, doc):
        # wait till the processing document count is smaller than the capped size
        retry = 300
        delay = 1
        while True:
            retry -= 1
            if retry == 0:
                logger.error('Failed to ingest after retries: %s', doc_id)
                return
            try:
                wds_result = self.discovery.get_collection(self.env_id, self.collection_id).get_result()
                if wds_result['document_counts']['processing'] < self.processing_cap:
                    break
                else:
                    time.sleep(delay)
            except:
                logger.exception('Unexpected exception when ingesting %s', doc_id)
                logger.error('Failed to ingest: %s', doc_id)
                return

        try:
            io = StringIO()
            json.dump(doc, io)
            io.seek(0)
            logger.info('Ingesting document %s', doc_id)
            wds_result = self.discovery.update_document(self.env_id, self.collection_id, doc_id, file=io, file_content_type='application/json', filename=file_name).get_result()
            logger.info('WDS response: %s', wds_result)
            assert 'processing' in wds_result['status']
            return wds_result
        except:
            logger.exception('Unexpected exception when ingesting %s', doc_id)
            logger.error('Failed to ingest: %s', doc_id)

    def delete_document(self, doc_id):
        logger.info('Deleting document %s', doc_id)
        wds_result = self.discovery.delete_document(self.env_id, self.collection_id, doc_id)
        logger.info('WDS response: %s', wds_result)
        return wds_result

    def get_document(self, doc_id, poll_delay=2):
        """
        :param doc_id:
        :param poll_delay: wait poll_delay seconds before next pull
        :return:
            Exception will be thrown by the underlying Watson Discovery API if doc_id is not found
        """
        logger.info('Getting document %s', doc_id)
        while True:
            wds_result = self.discovery.get_document_status(self.env_id, self.collection_id, doc_id).get_result()
            logger.info('WDS response: %s', wds_result)
            if 'available' in wds_result['status']:
                # pull the document
                qopts = {
                    'filter': '(id::"%s")' % doc_id
                }
                wds_result = self.discovery.query(self.env_id, self.collection_id, qopts)
                assert wds_result['matching_results'] == 1
                return wds_result['results'][0]
            elif 'processing' in wds_result['status']:
                time.sleep(poll_delay)
            else:
                logger.error('Unexpected status string: %s', wds_result['status'])
                raise Exception('Unexpected status')


def scan_left_over_files(path):
    """
    Recursively yield file DirEntry objects for the given `path` directory.
    :return: DirEntry
    """
    for entry in scandir(path):
        if entry.is_dir(follow_symlinks=False):
            for child in scan_left_over_files(entry.path):
                yield child
        else:
            yield entry


def parse_args():
    """
    Parse the command-line arguments and save the parsing result to the global (evil) variables
    :return: None
    """
    global logger

    parser = argparse.ArgumentParser(description='Ingest files to Watson Discovery Service (WDS)')

    parser.add_argument('--input', required=True, help='The input channel, file://directory, others')
    parser.add_argument('--output', required=False, help='Pull WDS enriched result to the output')
    parser.add_argument('--wds_cap', required=False, type=int, default=0,
                        help='The max WDS processing document counts.')  # when hit this, wait until process count reduce
    parser.add_argument('--concurrency', required=False, type=int, default=multiprocessing.cpu_count(),
                        help='How many concurrent threads. Default to 2 x number of CPUs')

    return parser.parse_args()


def consumer(ingest_output=None, wds_cap=None):
    """
    The consumer reads file change event from the task queue, and then do its work
    :return: None
    """
    logger.info('consumer %d started', threading.get_ident())
    wds_ingester = WDSIngester(os.getenv('WDS_USER'),
                               os.getenv('WDS_PASSWD'),
                               os.getenv('WDS_ENV_ID'),
                               os.getenv('WDS_COLLECTION_ID'),
                               os.getenv('WDS_VERSION'),
                               processing_cap=wds_cap)
    try:
        while True:
            priority, file_path = request_queue.get()

            if priority == CONST_Q_PRIORITY_POISON or file_path is None:
                logger.info('consumer %d gets POISON pills and is stopping' % threading.get_ident())
                break
            else:
                with codecs.open(file_path, 'r', 'utf-8') as f:
                    document_id = os.path.splitext(os.path.basename(file_path))[0]  # only keep the file_name w/o ext
                    file_content = f.read()
                    if len(file_content.strip()) == 0:
                        wds_ingester.delete_document(document_id)
                        if ingest_output:
                            #  TODO - delete document from the output directory
                            pass
                    else:
                        document = json.loads(file_content)
                        wds_ingester.update_document(document_id, os.path.basename(file_path), document)
                        if ingest_output:
                            enriched_document = wds_ingester.get_document(document_id)
                            output_filename = os.path.join(ingest_output, os.path.basename(file_path))
                            logger.info('Writing %s', output_filename)
                            with codecs.open(output_filename, mode='w', encoding='utf-8') as f:
                                f.write(json.dumps(enriched_document, ensure_ascii=False, indent=2))

                request_queue.task_done()
    except:
        # catch and log unexpected exception, so error handling can be enhanced gradually
        logger.exception('consumer %d exited due to unexpected exception' % threading.get_ident())


def main():
    args = parse_args()

    if args.concurrency > 1:
        kwarg = {
            "ingest_output": None,
            "wds_cap": args.wds_cap
        }
        for i in range(args.concurrency):
            t = Thread(target=consumer, kwargs=kwarg)
            t.start()
            thread_pool.append(t)
    else:
        wds_ingester = WDSIngester(os.getenv('WDS_USER'),
                                   os.getenv('WDS_PASSWD'),
                                   os.getenv('WDS_ENV_ID'),
                                   os.getenv('WDS_COLLECTION_ID'),
                                   os.getenv('WDS_VERSION'))

    for entry in scan_left_over_files(args.input):
        if entry.is_file():
            try:
                file_path = entry.path
                if args.concurrency > 1:
                    request_queue.put((CONST_Q_PRIORITY_UPDATE, file_path))
                else:
                    with codecs.open(file_path, 'r', 'utf-8') as f:
                        document_id = os.path.splitext(os.path.basename(file_path))[0]  # only keep the file_name w/o ext
                        file_content = f.read()
                        if len(file_content.strip()) == 0:
                            wds_ingester.delete_document(document_id)
                        else:
                            document = json.loads(file_content)
                            wds_ingester.update_document(document_id, os.path.basename(file_path), document)
            except:
                logger.exception('Error when processing %s' % entry)

    if args.concurrency > 1:
        for i in range(args.concurrency):
            request_queue.put((CONST_Q_PRIORITY_UPDATE, None))

        for i in range(args.concurrency):
            thread_pool[i].join()


#
# Main program
#
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

    #
    # Checking the required environment variables and initialize them
    #
    for required_env in ('WDS_USER', 'WDS_PASSWD', 'WDS_COLLECTION_ID', 'WDS_ENV_ID', 'WDS_VERSION'):
        if required_env not in os.environ:
            print('Missing required env variable "%s"' % required_env)
            sys.exit(-1)

    main()

    logger.info('Done. Exiting')
