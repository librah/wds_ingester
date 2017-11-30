import argparse
import codecs
import json
import logging
import os
from StringIO import StringIO
import sys
import time

from watson_developer_cloud import DiscoveryV1

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
# Evil global variables used in this code
#
ingest_input = None  # where to get the document for ingestion
ingest_output = None  # where to store the ingestion result


class WDSIngester:
    def __init__(self, user, password, env_id, collection_id, version):
        discovery_option = {
            'username': user,
            'password': password
        }
        self.discovery = DiscoveryV1(version, **discovery_option)
        self.env_id = env_id
        self.collection_id = collection_id
        self.version = version

        collection = self.discovery.get_collection(self.env_id, self.collection_id)
        logger.info('Creating WDSIngester to WDS collection: %s (%s)', collection['name'], collection['collection_id'])

    def update_document(self, doc_id, doc):
        io = StringIO()
        json.dump(doc, io)
        io.seek(0)
        logger.info('Ingesting document %s', doc_id)
        wds_result = self.discovery.update_document(self.env_id, self.collection_id, doc_id, file_data=io, mime_type='application/json')
        logger.info('WDS response: %s', wds_result)
        assert 'processing' in wds_result['status']
        return wds_result

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
            wds_result = self.discovery.get_document(self.env_id, self.collection_id, doc_id)
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
    parser.add_argument('--d', required=False, default=False, action='store_true', help='Run in background. Default No')
    parser.add_argument('--output', required=False, help='Pull WDS enriched result to the output')

    args = parser.parse_args()

    global ingest_input
    ingest_input = os.path.abspath(os.path.expanduser(args.input))

    global ingest_output
    ingest_output = os.path.abspath(os.path.expanduser(args.output))


def main():
    wds_ingester = WDSIngester(os.getenv('WDS_USER'),
                               os.getenv('WDS_PASSWD'),
                               os.getenv('WDS_ENV_ID'),
                               os.getenv('WDS_COLLECTION_ID'),
                               os.getenv('WDS_VERSION'))
    for entry in scan_left_over_files(ingest_input):
        if entry.is_file():
            try:
                file_path = entry.path
                with open(file_path, 'r') as f:
                    document = json.loads(f.read().decode('utf-8-sig'))
                    document_id = document['id']

                wds_ingester.update_document(document_id, document)

                if ingest_output:
                    enriched_document = wds_ingester.get_document(document_id)
                    output_filename = os.path.join(ingest_output, os.path.basename(file_path))
                    logger.info('Writing %s', output_filename)
                    with codecs.open(output_filename, mode='w', encoding='utf-8') as f:
                        f.write(json.dumps(enriched_document, ensure_ascii=False, indent=2))
            except:
                logger.exception('Error when processing %s' % entry)


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
            print 'Missing required env variable "%s"' % required_env
            sys.exit(-1)

    parse_args()

    main()

    logger.info('Done. Exiting')
