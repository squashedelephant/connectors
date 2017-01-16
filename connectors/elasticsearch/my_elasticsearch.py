
from json import dumps
from logging import getLogger
from sys import exc_info
from traceback import extract_tb

from elasticsearch.client import Elasticsearch, IndicesClient
from elasticsearch.exceptions import ConnectionError, NotFoundError
from elasticsearch.exceptions import RequestError

from connectors.elasticsearch.error import ElasticSearchError
from connectors.elasticsearch.error import ElasticSearchReadError
from connectors.elasticsearch.error import ElasticSearchWriteError
from connectors.elasticsearch.message import ElasticSearchRead
from connectors.elasticsearch.message import ElasticSearchWrite

log = getLogger(__name__)

class ESConnector:
    """
    as many MS will communicate with ElasticSearch, centralize access
    with this library
    """

    def __init__(self,
                 host=None,
                 port=9200,
                 timeout=10,
                 local_env=False):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.local_env = local_env
        self.es = None

    def _connect(self):
        """
        connect to a member of the ElasticSearch cluster
        """
        try:
            if self.local_env:
                self.es = Elasticsearch([{'host': self.host,
                                          'port': self.port}])
            else:
                self.es = Elasticsearch([{'host': self.host,
                                          'port': self.port}],
                                        sniff_on_start=True,
                                        sniff_on_connection_fail=True,
                                        sniffer_timeout=self.timeout)
            self.idx = IndicesClient(self.es)
            return
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchError.unknown_exception(backtrace, str(e))

    def _create_index(self, index, doc_type, settings=None, mappings=None):
        """
        create a new empty index
        mandatory args:
            index = index name 
            doc_type = document type, ie. any valid string
            settings = ElasticSearch cluster configuration
            mappings = dict of document fields by type and indexing preference
        """
        if not settings:
            settings = {'index': {'number_of_shards': '1',
                                  'number_of_replicas': '0'}}
        if not mappings:
            mappings = {'property': {'id': {'type': 'string',
                                           'index': 'not_analyzed'}}} 
        try:
            response = self.es.create(index=index,
                                      doc_type=doc_type,
                                      body=dumps(settings))
            self.idx.put_mapping(index=index,
                                 doc_type=doc_type,
                                 body=dumps(mappings))
            if not 'created' in response or not response['created']:
                return ElasticSearchError.unable_to_create_index(index)                                                                                                            
            log.info('Index: {} created'.format(index))
            log.info('ES create(): response: {}'.format(response))
            return
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except NotFoundError as e:
            return ElasticSearchError.missing_index(self.index)
        except RequestError as e:
            return ElasticSearchError.invalid_request(str(e))
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchError.unknown_exception(backtrace, str(e))

    def drop_index(self, index):
        try:
            if index in self.es.indices.stats()['indices'].keys():
                self.es.indices.delete(index=index, ignore=[400, 404])
            log.info('Index: {} deleted'.format(index))
            return
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except NotFoundError as e:
            return ElasticSearcheError.missing_index(self.index)
        except RequestError as e:
            return ElasticSearcheError.invalid_request(str(e))
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchError.unknown_exception(backtrace, str(e))

    def add_document(self, index=None, doc_type=None, doc_id=0, settings={}, mappings={}, values={}):
        """
        add a new document to an existing index
        mandatory args:
            index = index name 
            doc_type = document type, ie. any valid string
            settings = ElasticSearch cluster configuration
            mappings = dict of document fields by type and indexing preference
            values = dictionary of fields and values
        """
        try:
            err_msg = self._connect()
            if err_msg:
                return err_msg
            if index not in self.es.indices.stats()['indices'].keys():
                err_msg = self._create_index(index, doc_type, settings, mappings)
                if err_msg:
                    return err_msg
            response = self.es.create(index=index,
                                      doc_type=doc_type,
                                      id=doc_id,
                                      body=dumps(values))
            log.info('ES create(): response: {}'.format(response))
            return ElasticSearchWrite.object_created(response)
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except RequestError as e:
            return ElasticSearchError.invalid_request(str(e))
        except NotFoundError as e:
            return ElasticSearchError.missing_index(index)
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchWriteError.unknown_exception(doc_id, values, backtrace, str(e))

    def update_document(self, index, doc_type, doc_id, values):
        """
        update an existing document in an existing index
        mandatory args:
            index = index name 
            doc_type = document type, ie. any valid string
            doc_id = document_id
            values = dictionary of fields and values
        """
        try:
            err_msg = self._connect()
            if err_msg:
                return err_msg
            log.info('ES body: {}'.format(values))
            response = self.es.update(index=index,
                                      doc_type=doc_type,
                                      id=doc_id,
                                      body=dumps(values))
            log.info('ES update(): response: {}'.format(response))
            return ElasticSearchWrite.object_updated(response)
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except RequestError as e:
            return ElasticSearchError.invalid_request(str(e))
        except NotFoundError as e:
            return ElasticSearchError.missing_index(index)
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchWriteError.unknown_exception(doc_id, values, backtrace, str(e))

    def find_document(self, index, doc_type, dsl=None, fields=None):
        """
        find an existing document in an existing index
        mandatory args:
            index = index name 
            doc_type = document type, ie. any valid string
            dsl = query parameters in DSL format
            fields = list of fields to return
        """
        try:
            err_msg = self._connect()
            if err_msg:
                return err_msg
            response = self.es.search(index=index,
                                      doc_type=doc_type,
                                      body=dumps(dsl),
                                      _source=fields)
            return ElasticSearchRead.object_found(response)
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except RequestError as e:
            return ElasticSearchError.invalid_request(str(e))
        except NotFoundError as e:
            return ElasticSearchError.missing_index(index)
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchReadError.unknown_exception(dsl, fields, backtrace, str(e))

    def search_documents(self, index, doc_type, dsl, fields=None):
        """
        find an existing document in an existing index
        mandatory args:
            index = index name 
            doc_type = document type, ie. any valid string
            dsl = query parameters in DSL format
            fields = list of fields to return
        """
        try:
            err_msg = self._connect()
            if err_msg:
                return err_msg
            response = self.es.search(index=index,
                                      doc_type=doc_type,
                                      body=dumps(dsl),
                                      _source=fields)
            return ElasticSearchRead.objects_found(response)
        except ConnectionError as e:
            return ElasticSearchError.no_host_available(self.host, self.port)
        except RequestError as e:
            return ElasticSearchError.invalid_request(str(e))
        except NotFoundError as e:
            return ElasticSearchError.missing_index(index)
        except Exception as e:
            (type_e, value, traceback_prev) = exc_info()
            backtrace = extract_tb(traceback_prev)
            return ElasticSearchReadError.unknown_exception(dsl, fields, backtrace, str(e))
